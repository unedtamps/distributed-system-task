import sys
import time

import mysql.connector
from mysql.connector import errorcode, errors

DB_CONFIG = {
    "user": "root",
    "password": "rootpassword",
    "auth_plugin": "mysql_native_password",
}

NODES = [
    {"name": "Node 1", "host": "127.0.0.1", "port": 3306},
    {"name": "Node 2", "host": "127.0.0.1", "port": 3307},
    {"name": "Node 3", "host": "127.0.0.1", "port": 3308},
]


def get_connection(node, timeout=1):
    return mysql.connector.connect(
        **DB_CONFIG,
        host=node["host"],
        port=node["port"],
        connect_timeout=int(timeout),
        autocommit=True,
        connection_timeout=1,
        raise_on_warnings=False,
    )


def find_initial_primary():
    print("[INFO] Mencari Primary Node...")
    for node in NODES:
        try:
            conn = get_connection(node)
            cur = conn.cursor(dictionary=True)

            cur.execute(
                """
                SELECT MEMBER_HOST, MEMBER_ROLE
                FROM performance_schema.replication_group_members
                WHERE MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'
            """
            )
            row = cur.fetchone()

            cur.execute("SELECT @@read_only AS ro")
            ro = cur.fetchone()["ro"]

            cur.close()
            conn.close()

            if row and ro == 0:
                return node
        except:
            continue

    return None


def check_data_consistency(current_idx):
    out = []

    for node in NODES:
        try:
            conn = get_connection(node, timeout=0.5)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM app_db.failover_bench")
            count = cur.fetchone()[0]
            cur.close()
            conn.close()

            status = "OK" if count >= current_idx else "LAGGING"
            out.append(f"{node['name']}: {count} ({status})")

        except:
            out.append(f"{node['name']}: [UNREACHABLE]")

    print("   [SYNC CHECK] | " + " | ".join(out))


def run_continuous_failover_test():
    current_node = find_initial_primary()
    if not current_node:
        print("[FATAL] Cluster Down / Tidak ada Primary.")
        sys.exit(1)

    print(f"[INFO] Primary awal: {current_node['name']}")

    try:
        conn = get_connection(current_node)
        cur = conn.cursor()

        cur.execute("CREATE DATABASE IF NOT EXISTS app_db")
        cur.execute("DROP TABLE IF EXISTS app_db.failover_bench")
        cur.execute(
            """
            CREATE TABLE app_db.failover_bench (
                id INT PRIMARY KEY,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        cur.close()
    except Exception as e:
        print(f"[ERROR] Setup DB gagal: {e}")
        sys.exit(1)

    idx = 1

    print("\n--- MULAI PENGUJIAN ---")
    print("Coba docker stop / disconnect salah satu node untuk trigger failover.\n")

    while True:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO app_db.failover_bench (id) VALUES (%s)", (idx,))
            cur.close()

            print(f"[WRITE] ID {idx} OK pada {current_node['name']}")
            check_data_consistency(idx)

            idx += 1
            time.sleep(0.7)

        except (errors.OperationalError, errors.InterfaceError) as err:
            print(f"\n[FAILOVER] Lost connection ke {current_node['name']}!")
            print(f"[ERR] {err}")

            failover_start = time.time()
            conn.close()

            new_conn = None

            while new_conn is None:
                for node in NODES:
                    try:
                        tmp = get_connection(node, timeout=1)
                        cur = tmp.cursor()

                        cur.execute(
                            "INSERT INTO app_db.failover_bench (id) VALUES (%s)",
                            (idx,),
                        )
                        cur.close()

                        failover_end = time.time()
                        new_conn = tmp
                        current_node = node

                        print("\n[RECOVERED] Failover selesai.")
                        print(f"Primary baru: {node['name']}")
                        print(f"Downtime: {failover_end - failover_start:.4f} detik")

                        check_data_consistency(idx)
                        idx += 1
                        break

                    except:
                        pass

                if new_conn is None:
                    print(".", end="", flush=True)
                    time.sleep(0.4)

            conn = new_conn


if __name__ == "__main__":
    run_continuous_failover_test()
