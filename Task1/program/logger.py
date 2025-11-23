#!/usr/bin/env python3
import argparse, json, socket, time
from collections import defaultdict, deque

def vc_leq(a: dict, b: dict) -> bool:
    """a <= b componentwise (missing keys treated as 0)."""
    keys = set(a.keys()) | set(b.keys())
    for k in keys:
        if a.get(k, 0) > b.get(k, 0):
            return False
    return True

def vc_lt(a: dict, b: dict) -> bool:
    """a < b iff a <= b and a != b."""
    return vc_leq(a, b) and any(a.get(k,0) < b.get(k,0) for k in set(a)|set(b))

def topo_sort_by_vc(events):
    """
    Build a partial order using vector clocks (edges i->j if VC_i < VC_j).
    Return a topological order; break ties by (sender, id) for concurrent events.
    """
    n = len(events)
    edges = defaultdict(set)
    indeg = [0]*n

    for i in range(n):
        for j in range(n):
            if i == j: continue
            if vc_lt(events[i]["vclock"], events[j]["vclock"]):
                if j not in edges[i]:
                    edges[i].add(j)
                    indeg[j] += 1

    # Kahn's algorithm with stable tie-break
    q = deque(sorted([i for i in range(n) if indeg[i]==0],
                     key=lambda k: (events[k]["sender"], events[k]["id"])))
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in sorted(edges[u],
                        key=lambda k: (events[k]["sender"], events[k]["id"])):
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    # If we couldn't order all (shouldn't happen; concurrency just means no edge)
    if len(order) < n:
        # append remaining in stable order
        remaining = [i for i in range(n) if i not in order]
        remaining.sort(key=lambda k: (events[k]["sender"], events[k]["id"]))
        order.extend(remaining)
    return [events[i] for i in order]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9999)
    ap.add_argument("--expect", type=int, default=2,
                    help="How many chat messages to wait for before printing orders")
    args = ap.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.bind, args.port))
    sock.settimeout(0.5)
    print(f"[COLLECTOR] Listening on {(args.bind, args.port)}")

    events = []
    t0 = time.monotonic()
    while True:
        try:
            data, addr = sock.recvfrom(65535)
        except socket.timeout:
            if len(events) >= args.expect and (time.monotonic() - t0) > 0.5:
                break
            continue

        try:
            msg = json.loads(data.decode())
        except Exception as e:
            print(f"[COLLECTOR] Bad JSON from {addr}: {e}")
            continue

        if msg.get("type") == "chat":
            msg["_collector_arrival"] = time.time()
            events.append(msg)
            print(f"[COLLECTOR] CHAT {msg}")
            if len(events) >= args.expect:
                time.sleep(0.2)
                break

    if not events:
        print("[COLLECTOR] No events collected.")
        return

    # 1) Naive (sender wall clock)
    naive = sorted(events, key=lambda m: m["local_ts"])

    # 2) Lamport (total order via L, tie-break by sender)
    lamport_sorted = sorted(events, key=lambda m: (m["lamport"], m["sender"]))

    # 3) Vector-clock causal topological order
    vc_sorted = topo_sort_by_vc(events)

    print("\n=== Naive order (by sender local_ts) — may violate causality ===")
    for m in naive:
        print(f"{m['id']} {m['sender']}->{m['receiver']} local_ts={m['local_ts']:.3f} "
              f"L={m['lamport']} V={m['vclock']} \"{m['text']}\"")

    print("\n=== Causal order (Lamport total order) ===")
    for m in lamport_sorted:
        print(f"{m['id']} {m['sender']}->{m['receiver']} L={m['lamport']} "
              f"V={m['vclock']} \"{m['text']}\"")

    print("\n=== Causal order (Vector-clock topological order) ===")
    for m in vc_sorted:
        print(f"{m['id']} {m['sender']}->{m['receiver']} V={m['vclock']} "
              f"L={m['lamport']} \"{m['text']}\"")

    # Pairwise VC relationship summary (useful to show concurrency)
    if len(events) >= 2:
        print("\n=== Vector-clock relations (pairwise) ===")
        for i in range(len(events)):
            for j in range(i+1, len(events)):
                a, b = events[i], events[j]
                a_lt_b = vc_lt(a["vclock"], b["vclock"])
                b_lt_a = vc_lt(b["vclock"], a["vclock"])
                if a_lt_b:
                    rel = f"{a['id']} -> {b['id']} (a happens-before b)"
                elif b_lt_a:
                    rel = f"{b['id']} -> {a['id']} (b happens-before a)"
                else:
                    rel = f"{a['id']} || {b['id']} (concurrent)"
                print(rel)

if __name__ == "__main__":
    main()

