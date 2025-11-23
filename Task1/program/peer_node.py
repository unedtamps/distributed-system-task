#!/usr/bin/env python3
# peer.py — interactive multi-peer UDP chat with Lamport + Vector clocks
import argparse, json, socket, time, sys, threading, queue
from typing import Dict, Tuple

def parse_peer_token(tok: str) -> Tuple[str, Tuple[str, int]]:
    """
    Parse NAME@HOST:PORT into ("NAME", ("HOST", PORT)).
    Example: A@10.0.0.11:5000
    """
    try:
        name_part, hp = tok.split("@", 1)
        host_part, port_part = hp.rsplit(":", 1)
        return name_part.strip(), (host_part.strip(), int(port_part))
    except Exception as e:
        raise ValueError(f"Invalid --peers token '{tok}'; expected NAME@HOST:PORT") from e

def now_local_with_offset(offset_s: float) -> float:
    return time.time() + offset_s

class PeerNode:
    def __init__(
        self,
        name: str,
        listen_host: str,
        listen_port: int,
        peers: Dict[str, Tuple[str, int]],
        logger: Tuple[str, int] | None,
        offset_ms: int,
        proc_delay_ms: int,
        initiate_to: str | None,
        initiate_broadcast: bool,
        init_text: str,
        default_to: str | None,
    ):
        self.name = name
        self.listen = (listen_host, listen_port)
        self.peers: Dict[str, Tuple[str, int]] = {n: a for n, a in peers.items() if n != name}
        self.logger = logger
        self.offset_s = offset_ms / 1000.0
        self.proc_delay_s = proc_delay_ms / 1000.0
        self.initiate_to = initiate_to
        self.initiate_broadcast = initiate_broadcast
        self.init_text = init_text
        self.default_to = default_to

        # Logical clocks
        self.L = 0  # Lamport
        self.V: Dict[str, int] = {self.name: 0}
        for n in self.peers.keys():
            self.V.setdefault(n, 0)

        # Sockets & I/O
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.listen)
        self.sock.settimeout(0.1)

        # Interactive input queue (lines from stdin)
        self.input_q: "queue.Queue[str]" = queue.Queue()
        self._stop = threading.Event()

        self._print_banner()

    # ------------- Utilities -------------
    def _print(self, s: str):
        print(s, flush=True)

    def _print_banner(self):
        self._print(f"[{self.name}] Listening on {self.listen}")
        self._print(f"[{self.name}] Peers: " + (", ".join(f"{n}@{h}:{p}" for n,(h,p) in self.peers.items()) or "(none)"))
        if self.logger:
            self._print(f"[{self.name}] Collector: {self.logger}")
        self._print(f"[{self.name}] Clock offset={self.offset_s:+.3f}s, proc_delay={self.proc_delay_s*1000:.0f}ms")
        self._print(
            "\nType messages:\n"
            "  @NAME your message      -> send to one peer\n"
            "  /broadcast your message -> send to all peers\n"
            "  /peers  /help  /exit\n"
            + (f"  (Bare lines go to {self.default_to})\n" if self.default_to else "")
        )

    # ------------- Lamport -------------
    def l_on_send(self) -> int:
        self.L += 1
        return self.L

    def l_on_receive(self, incoming_L: int) -> int:
        self.L = max(self.L, incoming_L) + 1
        return self.L

    # ------------- Vector -------------
    def _ensure_v_keys(self, incoming: Dict[str, int]):
        for k in incoming.keys():
            if k not in self.V:
                self.V[k] = 0

    def v_on_send(self) -> Dict[str, int]:
        self.V[self.name] = self.V.get(self.name, 0) + 1
        return dict(self.V)

    def v_on_receive(self, incoming_V: Dict[str, int]) -> Dict[str, int]:
        self._ensure_v_keys(incoming_V)
        for k in set(self.V.keys()).union(incoming_V.keys()):
            self.V[k] = max(self.V.get(k, 0), incoming_V.get(k, 0))
        self.V[self.name] = self.V.get(self.name, 0) + 1
        return dict(self.V)

    # ------------- Messaging -------------
    def _make_payload(self, kind: str, msg_id: str, to_name: str, text: str) -> bytes:
        L = self.l_on_send()
        V = self.v_on_send()
        payload = {
            "type": "chat",
            "kind": kind,   # "chat" or "ack"
            "id": msg_id,
            "sender": self.name,
            "receiver": to_name,
            "text": text,
            "lamport": L,
            "vclock": V,
            "local_ts": now_local_with_offset(self.offset_s),
            "mono_send": time.monotonic(),
        }
        return json.dumps(payload).encode()

    def _send_to(self, to_name: str, raw: bytes):
        if to_name not in self.peers:
            self._print(f"[{self.name}] WARN: unknown peer '{to_name}'")
            return
        self.sock.sendto(raw, self.peers[to_name])
        if self.logger:
            self.sock.sendto(raw, self.logger)

    def send_chat(self, to_name: str, text: str):
        msg_id = f"CHAT-{self.name}-{int(time.monotonic()*1000)}"
        self._send_to(to_name, self._make_payload("chat", msg_id, to_name, text))
        self._print(f"[{self.name}] SENT chat -> {to_name}: id={msg_id} text='{text}'")

    def send_ack(self, to_name: str, correlate_id: str):
        msg_id = f"ACK-{self.name}-{int(time.monotonic()*1000)}"
        self._send_to(to_name, self._make_payload("ack", msg_id, to_name, f"Ack:{correlate_id}"))
        self._print(f"[{self.name}] SENT ack  -> {to_name}: id={msg_id} for={correlate_id}")

    # ------------- Interactive input -------------
    def _stdin_thread(self):
        try:
            while not self._stop.is_set():
                line = sys.stdin.readline()
                if not line:
                    time.sleep(0.05)
                    continue
                self.input_q.put(line.rstrip("\n"))
        except Exception as e:
            self._print(f"[{self.name}] stdin thread error: {e}")

    def _handle_user_line(self, line: str):
        s = line.strip()
        if not s:
            return
        if s.startswith("/"):
            cmd, *rest = s.split(maxsplit=1)
            arg = rest[0] if rest else ""
            if cmd == "/broadcast":
                if not self.peers:
                    self._print(f"[{self.name}] No peers to broadcast to.")
                    return
                for to in list(self.peers.keys()):
                    self.send_chat(to, arg or "(empty)")
                return
            if cmd == "/peers":
                self._print("Peers: " + (", ".join(self.peers.keys()) or "(none)"))
                return
            if cmd in ("/exit", "/quit"):
                self._print(f"[{self.name}] Exiting on user request.")
                self._stop.set()
                return
            if cmd == "/help":
                self._print("Commands: /broadcast TEXT | /peers | /help | /exit\n"
                            "Targeted send: @NAME TEXT  (or bare line if --default-to NAME was set)")
                return
            self._print(f"Unknown command '{cmd}'. Try /help")
            return

        if s.startswith("@"):
            # @NAME message
            try:
                dest, msg = s[1:].split(maxsplit=1)
            except ValueError:
                self._print("Usage: @NAME your message")
                return
            self.send_chat(dest, msg)
            return

        # Bare line: use default target if configured
        if self.default_to:
            self.send_chat(self.default_to, s)
        else:
            self._print("No default target. Use @NAME message or set --default-to NAME.")

    # ------------- Initiation helpers -------------
    def _maybe_initiate_once(self, started_at: float):
        # Trigger once ~0.5s after start
        if time.monotonic() - started_at <= 0.5:
            return
        if self.initiate_to:
            if self.initiate_to != self.name:
                self.send_chat(self.initiate_to, self.init_text or f"Hi {self.initiate_to} — are you there?")
            self.initiate_to = None
        elif self.initiate_broadcast:
            for to_name in list(self.peers.keys()):
                self.send_chat(to_name, self.init_text or f"Hi {to_name} — are you there?")
                time.sleep(0.05)
            self.initiate_broadcast = False

    # ------------- Main loop -------------
    def run(self):
        # Start stdin reader thread
        t_in = threading.Thread(target=self._stdin_thread, daemon=True)
        t_in.start()

        started_at = time.monotonic()
        try:
            while not self._stop.is_set():
                # First, run any one-time initiation
                self._maybe_initiate_once(started_at)

                # Handle inbound socket
                try:
                    data, addr = self.sock.recvfrom(65535)
                except socket.timeout:
                    data = None
                if data:
                    try:
                        msg = json.loads(data.decode())
                    except Exception as e:
                        self._print(f"[{self.name}] Bad JSON from {addr}: {e}")
                        msg = None

                    if msg and msg.get("type") == "chat" and msg.get("receiver") == self.name:
                        inL = int(msg.get("lamport", 0))
                        inV = msg.get("vclock", {})
                        self.l_on_receive(inL)
                        self.v_on_receive(inV)
                        self._print(f"[{self.name}] RECV {msg.get('kind','?')} "
                                    f"from {msg.get('sender')} id={msg.get('id')} "
                                    f"L_in={inL}->{self.L} V_in={inV}->{self.V}")
                        if msg.get("kind") == "chat":
                            time.sleep(self.proc_delay_s)
                            self.send_ack(msg["sender"], msg["id"])

                # Handle user input lines
                try:
                    line = self.input_q.get_nowait()
                except queue.Empty:
                    line = None
                if line is not None:
                    self._handle_user_line(line)

        except KeyboardInterrupt:
            self._print(f"\n[{self.name}] Interrupted. Exiting.")
        finally:
            self._stop.set()
            try:
                self.sock.close()
            except Exception:
                pass

def parse_args():
    ap = argparse.ArgumentParser(description="Interactive multi-peer UDP chat with Lamport + Vector clocks")
    ap.add_argument("--name", required=True, help="This node's name (e.g., A, B, C)")
    ap.add_argument("--listen", nargs=2, metavar=("HOST","PORT"), required=True, help="Local bind host/port")
    ap.add_argument("--peers", nargs="+", metavar="NAME@HOST:PORT", required=True,
                    help="List of peers as NAME@HOST:PORT (include self too if convenient)")
    ap.add_argument("--logger", nargs=2, metavar=("HOST","PORT"), help="Collector host/port (optional)")
    ap.add_argument("--offset-ms", type=int, default=0, help="Simulated clock skew")
    ap.add_argument("--proc-delay-ms", type=int, default=10, help="Processing delay for ACK")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--initiate-to", metavar="NAME", help="Send initial message to a single peer")
    grp.add_argument("--initiate-broadcast", action="store_true", help="Broadcast initial message to all peers")
    ap.add_argument("--msg", default="", help="Initial message text (optional)")
    ap.add_argument("--default-to", metavar="NAME", help="Default recipient for bare input lines")
    args = ap.parse_args()

    lh, lp = args.listen[0], int(args.listen[1])
    peers_map = dict(parse_peer_token(t) for t in args.peers)
    logger = (args.logger[0], int(args.logger[1])) if args.logger else None

    return args, lh, lp, peers_map, logger

if __name__ == "__main__":
    args, lh, lp, peers_map, logger = parse_args()
    PeerNode(
        name=args.name,
        listen_host=lh, listen_port=lp,
        peers=peers_map,
        logger=logger,
        offset_ms=args.offset_ms,
        proc_delay_ms=args.proc_delay_ms,
        initiate_to=args.initiate_to,
        initiate_broadcast=args.initiate_broadcast,
        init_text=args.msg.strip(),
        default_to=args.default_to,
    ).run()

