#!/usr/bin/env python3
from __future__ import annotations
import argparse, socket, threading, time, json, random, sys
from typing import Dict, Tuple, List, Optional

STATE_ALIVE, STATE_SUSPECT, STATE_DEAD = 'ALIVE','SUSPECT','DEAD'

# ------------------------------- Utility ---------------------------------

def recv_all(conn: socket.socket) -> str:
    conn.settimeout(3)
    chunks=[]
    try:
        while True:
            b=conn.recv(65535)
            if not b: break
            chunks.append(b)
    except Exception:
        pass
    return b''.join(chunks).decode().strip()

# ------------------------------- Logger ----------------------------------

class Logger:
    """Collects events from nodes and prints orders by physical, Lamport, Vector."""
    def __init__(self, tcp_port: int, numnodes: int, interval: float=3.0):
        self.tcp_port=tcp_port
        self.n=numnodes
        self.events: List[Dict] = []
        self.lock=threading.Lock()
        self.interval=interval

    def serve(self):
        threading.Thread(target=self._printer, daemon=True).start()
        srv=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('0.0.0.0', self.tcp_port)); srv.listen(128)
        print(f"[LOGGER] listening on {self.tcp_port}")
        while True:
            conn,_=srv.accept()
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket):
        try:
            raw=recv_all(conn)
            if not raw: return
            ev=json.loads(raw)
            with self.lock:
                self.events.append(ev)
        except Exception:
            pass
        finally:
            try: conn.close()
            except: pass

    @staticmethod
    def _vc_leq(a: List[int], b: List[int]) -> bool:
        return all(x<=y for x,y in zip(a,b))

    def _printer(self):
        while True:
            time.sleep(self.interval)
            with self.lock:
                evs=list(self.events)
            if not evs:
                continue
            phys=sorted(evs, key=lambda e:(e['phy_ts'], e['node'], e['lamport']))
            lam =sorted(evs, key=lambda e:(e['lamport'], e['node']))

            # Vector layers (set of incomparable events)
            used=set(); layers: List[List[Dict]]=[]
            while len(used)<len(evs):
                layer=[]
                for i,e in enumerate(evs):
                    if i in used: continue
                    before=False
                    for j,o in enumerate(evs):
                        if j in used or j==i: continue
                        if self._vc_leq(o['vector'], e['vector']) and o['vector']!=e['vector']:
                            before=True; break
                    if not before: layer.append(e)
                if not layer: break
                layers.append(layer)
                for e in layer: used.add(evs.index(e))

            print("\n================ TRACE (last %d events) ================"%len(evs))
            print("-- Physical order --")
            for e in phys:
                print(f"t={e['phy_ts']:.6f} L={e['lamport']:>3} V={e['vector']} node={e['node']} {e['stage']} {e['op']}")
            print("-- Lamport order --")
            for e in lam:
                print(f"L={e['lamport']:>3} t={e['phy_ts']:.6f} V={e['vector']} node={e['node']} {e['stage']} {e['op']}")
            print("-- Vector partial order (layers of concurrent sets) --")
            for i,layer in enumerate(layers,1):
                desc=", ".join([f"n{e['node']}:{e['stage']}:{e['op']}@{e['vector']}" for e in layer])
                print(f"Layer {i}: {desc}")
            print("======================================================\n")

# ------------------------------- KV Node ---------------------------------

class KV:
    def __init__(self):
        self.store: Dict[str, Tuple[float,str]]={}
        self.lock=threading.Lock()
    def put(self,k,v):
        ts=time.monotonic()
        with self.lock:
            cur=self.store.get(k)
            if not cur or ts>=cur[0]:
                self.store[k]=(ts,v)
    def get(self,k)->str:
        with self.lock:
            return self.store.get(k,(0.0,'<nil>'))[1]

class Gossip:
    """
    peers_map: List[(host, tcp, udp, id)]
    Sends gossip to each peer's UDP (critical fix).
    """
    def __init__(self, node_id:int, udp_port:int, peers_map:List[Tuple[str,int,int,int]]):
        self.id=node_id; self.udp_port=udp_port
        self.table: Dict[int, Dict]= {
            self.id:{'state':STATE_ALIVE,'hb':0,'last':time.monotonic(),'addr':('127.0.0.1',None)}
        }
        for h,tcp,udp,nid in peers_map:
            self.table[nid]={'state':STATE_SUSPECT,'hb':0,'last':time.monotonic(),'addr':(h,tcp)}
        self.peers_map=peers_map
        self.sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0',udp_port))
        threading.Thread(target=self._rx,daemon=True).start()
        threading.Thread(target=self._tx,daemon=True).start()

    def _rx(self):
        while True:
            try:
                data,addr=self.sock.recvfrom(65535)
                msg=json.loads(data.decode())
                if msg.get('type')!='gossip': continue
                sid=msg['from']; hb=msg.get('heartbeat',0)
                now=time.monotonic()
                self.table.setdefault(sid,{'state':STATE_SUSPECT,'hb':0,'last':now,'addr':(addr[0],None)})
                s=self.table[sid]; s['state']=STATE_ALIVE; s['hb']=max(s['hb'],hb); s['last']=now
                for nid_s,rec in msg.get('known',{}).items():
                    nid=int(nid_s)
                    if nid==self.id: continue
                    self.table.setdefault(nid,{'state':STATE_SUSPECT,'hb':0,'last':now,'addr':tuple(rec.get('addr')) if rec.get('addr') else (None,None)})
                    loc=self.table[nid]
                    loc['hb']=max(loc['hb'], rec.get('hb',0))
                    if rec.get('state')==STATE_DEAD: loc['state']=STATE_DEAD
                    elif rec.get('state')==STATE_ALIVE and loc['state']!=STATE_DEAD: loc['state']=STATE_ALIVE
                    if rec.get('addr'): loc['addr']=tuple(rec['addr'])
            except Exception:
                pass

    def _tx(self):
        while True:
            time.sleep(0.5)
            now=time.monotonic(); me=self.table[self.id]
            me['hb']=me.get('hb',0)+1; me['last']=now; me['state']=STATE_ALIVE
            # suspicion / death
            for nid,inf in list(self.table.items()):
                if nid==self.id: continue
                age=now-inf['last']
                if age>5.0: inf['state']=STATE_DEAD
                elif age>2.0 and inf['state']==STATE_ALIVE: inf['state']=STATE_SUSPECT
            msg={'type':'gossip','from':self.id,'heartbeat':me['hb'],'known':{str(n):{'state':inf['state'],'hb':inf['hb'],'addr':list(inf.get('addr',('127.0.0.1',None)))} for n,inf in self.table.items()}}
            targets=random.sample(self.peers_map, k=min(2, len(self.peers_map))) if self.peers_map else []
            for h,_tcp,peer_udp,_nid in targets:
                try: self.sock.sendto(json.dumps(msg).encode(), (h, peer_udp))
                except Exception: pass

    def leader(self)->Optional[int]:
        alive=[nid for nid,inf in self.table.items() if inf['state']==STATE_ALIVE]
        return max(alive) if alive else None

    def addr_of(self,nid:int)->Optional[Tuple[str,int]]:
        for h,tcp,_udp,i in self.peers_map:
            if i==nid: return (h,tcp)
        if nid==self.id:
            for h,tcp,_udp,i in self.peers_map:
                if i==self.id: return (h,tcp)
        return None

class MutexCoordinator:
    def __init__(self):
        self.lock=threading.Lock(); self.held_by: Optional[int]=None; self.queue: List[int]=[]
    def req(self,nid:int)->bool:
        with self.lock:
            if self.held_by is None:
                self.held_by=nid; return True
            if nid not in self.queue: self.queue.append(nid)
            return False
    def rel(self,nid:int)->Optional[int]:
        with self.lock:
            if self.held_by==nid:
                self.held_by=None
                if self.queue:
                    nxt=self.queue.pop(0); self.held_by=nxt; return nxt
        return None

class Node:
    def __init__(self, node_id:int, tcp_port:int, udp_port:int, peers_map:List[Tuple[str,int,int,int]],
                 logger_addr:Tuple[str,int], numnodes:int, use_mutex:bool):
        self.id=node_id; self.tcp_port=tcp_port; self.use_mutex=use_mutex
        # Ensure self is present with its UDP and TCP
        if not any(i==node_id for *_, i in peers_map):
            peers_map=[('127.0.0.1', tcp_port, udp_port, node_id)] + peers_map
        self.gossip=Gossip(node_id, udp_port, peers_map)
        self.coord=MutexCoordinator()
        self.kv=KV()
        self.logger_addr=logger_addr
        self.n=numnodes; self.idx = node_id-1  # expecting ids 1..n
        self.lamport=0
        self.vector=[0]*numnodes
        threading.Thread(target=self.tcp_server,daemon=True).start()
        threading.Thread(target=self.status_loop,daemon=True).start()
        threading.Thread(target=self.interactive_loop, daemon=True).start()

    # ------- Clocks & logging -------
    def _tick_local(self):
        self.lamport += 1
        self.vector[self.idx] += 1
    def _merge_on_recv(self, lam:int, vec:List[int]):
        self.lamport = max(self.lamport, lam) + 1
        for i in range(self.n):
            self.vector[i]=max(self.vector[i], vec[i])
        self.vector[self.idx]+=1
    def _log(self, stage:str, op:str):
        ev={'node': self.id, 'stage': stage, 'op': op,
            'phy_ts': time.time(), 'lamport': self.lamport, 'vector': list(self.vector)}
        try:
            s=socket.create_connection(self.logger_addr, timeout=0.3)
            s.sendall((json.dumps(ev)+'\n').encode()); s.close()
        except Exception:
            pass

    # ------- TCP server (client & RPCs) -------
    def tcp_server(self):
        srv=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('0.0.0.0', self.tcp_port)); srv.listen(128)
        print(f"[node {self.id}] TCP {self.tcp_port} | use_mutex={self.use_mutex}")
        while True:
            conn,_=srv.accept()
            threading.Thread(target=self.handle_conn, args=(conn,), daemon=True).start()

    def handle_conn(self, conn: socket.socket):
        try:
            raw=recv_all(conn)
            if not raw: conn.sendall(b"ERR\n"); return
            parts=raw.split()
            cmd=parts[0].upper()
            if cmd=='GET' and len(parts)==2:
                self._tick_local(); self._log('GET', parts[1])
                conn.sendall((self.kv.get(parts[1])+'\n').encode()); return
            if cmd=='PUT' and len(parts)>=3:
                key, val = parts[1], " ".join(parts[2:])
                self._do_put(key, val)
                conn.sendall(b"OK\n"); return
            if cmd=='REPL_PUT' and len(parts)>=5:
                # REPL_PUT k v lam json_vector
                key=parts[1]; val=parts[2]; rlam=int(parts[3]); rvec=json.loads(" ".join(parts[4:]))
                self._merge_on_recv(rlam, rvec); self._log('REPL_RECV', f"{key}={val}")
                self.kv.put(key,val)
                conn.sendall(b"OK\n"); return
            if cmd=='LOCK_REQ' and len(parts)==2:
                nid=int(parts[1]); granted=self.coord.req(nid)
                conn.sendall((b"GRANTED\n" if granted else b"QUEUED\n")); return
            if cmd=='LOCK_REL' and len(parts)==2:
                nid=int(parts[1]); self.coord.rel(nid); conn.sendall(b"OK\n"); return
            conn.sendall(b"ERR\n")
        finally:
            try: conn.close()
            except: pass

    # ------- Local helpers (used by server & interactive) -------
    def _do_put(self, key: str, val: str):
        if self.use_mutex:
            self._tick_local(); self._log('MUTEX_REQ', key)
            self._acquire_mutex(); self._log('MUTEX_GOT', key)
        self._tick_local(); self._log('APPLY_LOCAL', f"{key}={val}")
        self.kv.put(key,val)
        self._tick_local(); self._log('REPL_SEND', f"{key}={val}")
        self._replicate_put(key,val)
        if self.use_mutex:
            self._tick_local(); self._release_mutex(); self._log('MUTEX_REL', key)

    # ------- Replication -------
    def _replicate_put(self, k, v):
        payload=f"REPL_PUT {k} {v} {self.lamport} {json.dumps(self.vector)}\n".encode()
        for h,tcp,_udp,i in self.gossip.peers_map:
            if i==self.id: continue
            try:
                s=socket.create_connection((h,tcp), timeout=0.4)
                s.sendall(payload); s.close()
            except Exception:
                pass

    # ------- Distributed mutex via leader -------
    def _acquire_mutex(self):
        while True:
            leader=self.gossip.leader()
            if leader is None:
                time.sleep(0.05); continue
            if leader==self.id:
                if self.coord.req(self.id): return
                time.sleep(0.05); continue
            addr=self.gossip.addr_of(leader)
            if not addr:
                time.sleep(0.05); continue
            try:
                s=socket.create_connection(addr, timeout=0.5)
                s.sendall(f"LOCK_REQ {self.id}\n".encode()); s.shutdown(socket.SHUT_WR)
                resp=recv_all(s); s.close()
                if resp.strip()=="GRANTED": return
            except Exception: pass
            time.sleep(0.05)

    def _release_mutex(self):
        leader=self.gossip.leader()
        if leader==self.id:
            self.coord.rel(self.id); return
        addr=self.gossip.addr_of(leader)
        if not addr: return
        try:
            s=socket.create_connection(addr, timeout=0.5)
            s.sendall(f"LOCK_REL {self.id}\n".encode()); s.close()
        except Exception: pass

    # ------- Periodic status -------
    def status_loop(self):
        while True:
            leader=self.gossip.leader()
            print(f"[node {self.id}] leader={leader} color={self.kv.get('color')} L={self.lamport} V={self.vector}")
            time.sleep(1.0)

    # ------- Interactive input on each node -------
    def interactive_loop(self):
        print(f"[node {self.id}] Interactive ready. Type: GET <key> | PUT <key> <value> | help")
        while True:
            try:
                line=input("").strip()
            except EOFError:
                return
            if not line:
                continue
            if line.lower()=="help":
                print("Commands: GET <key> | PUT <key> <value>")
                continue
            parts=line.split()
            cmd=parts[0].upper()
            if cmd=="GET" and len(parts)==2:
                val=self.kv.get(parts[1])
                print(f"GET {parts[1]} -> {val}")
            elif cmd=="PUT" and len(parts)>=3:
                key=parts[1]; val=" ".join(parts[2:])
                self._do_put(key,val)
                print("OK")
            else:
                print("Unknown/invalid. Type 'help'.")

# --------------------------------- Main ----------------------------------

def parse_peers(s: str) -> List[Tuple[str,int,int,int]]:
    """
    Accepts either host:tcp=id  or host:tcp:udp=id
    If udp omitted, infer udp=tcp+100
    Returns list of (host, tcp, udp, id)
    """
    out=[]
    if not s: return out
    for tok in s.split(','):
        hp, nid_s = tok.split('=')
        parts = hp.split(':')
        if len(parts)==2:
            h, tcp_s = parts
            udp_s = str(int(tcp_s)+100)
        else:
            h, tcp_s, udp_s = parts
        out.append((h, int(tcp_s), int(udp_s), int(nid_s)))
    return out

def main():
    ap=argparse.ArgumentParser(description='KV store with optional gossip-mutex and logger (final)')
    ap.add_argument('--logger', action='store_true', help='Run as logger node')
    ap.add_argument('--logger-tcp', type=int, default=9000)
    ap.add_argument('--numnodes', type=int, default=3)

    ap.add_argument('--id', type=int)
    ap.add_argument('--tcp', type=int)
    ap.add_argument('--udp', type=int)
    ap.add_argument('--peers', type=str, default='', help='host:tcp=id or host:tcp:udp=id (others; self auto-added)')
    ap.add_argument('--logger-addr', type=str, default='127.0.0.1:9000')
    ap.add_argument('--use-mutex', type=int, default=0, help='0/1 to disable/enable mutex')

    args=ap.parse_args()

    if args.logger:
        Logger(args.logger_tcp, args.numnodes).serve()
        return

    if not all([args.id, args.tcp, args.udp]):
        print('Run as node requires --id --tcp --udp', file=sys.stderr); sys.exit(2)

    peers=parse_peers(args.peers)
    la_h, la_p = args.logger_addr.split(':'); logger_addr=(la_h,int(la_p))

    Node(args.id, args.tcp, args.udp, peers, logger_addr, args.numnodes, bool(args.use_mutex))
    # Keep process alive
    while True:
        time.sleep(3600)

if __name__=='__main__':
    main()

