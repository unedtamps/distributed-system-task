import sys
import time

import mysql.connector

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
        **DB_CONFIG, host=node["host"], port=node["port"], connect_timeout=timeout
    )


def find_initial_primary():
    print("[INFO] Mencari Primary Node...")
    for node in NODES:
        try:
            conn = get_connection(node)
            cur = conn.cursor(dictionary=True)
            cur.execute(
                "SELECT MEMBER_HOST, MEMBER_ROLE FROM performance_schema.replication_group_members "
                "WHERE MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'"
            )
            row = cur.fetchone()
            cur.execute("SELECT @@read_only")
            is_read_only = cur.fetchone()["@@read_only"]

            conn.close()

            if row and is_read_only == 0:
                return node
        except:
            continue
    return None


def check_data_consistency(current_idx):
    status_output = []

    for node in NODES:
        try:
            conn = get_connection(node, timeout=0.5)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM app_db.failover_bench")
            count = cur.fetchone()[0]
            conn.close()
            sync_status = "OK" if count >= current_idx else "LAGGING"
            status_output.append(f"{node['name']}: {count} ({sync_status})")

        except mysql.connector.Error:
            status_output.append(f"{node['name']}: [UNREACHABLE]")
    print(f"   [SYNC CHECK] | {' | '.join(status_output)}")


def run_continuous_failover_test():
    current_node = find_initial_primary()

    if not current_node:
        print("[FATAL] Cluster Down atau tidak ada Primary.")
        sys.exit(1)

    print(f"[INFO] Terhubung ke Primary: {current_node['name']}")

    conn = None
    try:
        conn = get_connection(current_node)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS app_db")
        cur.execute("DROP TABLE IF EXISTS app_db.failover_bench")
        cur.execute(
            "CREATE TABLE app_db.failover_bench (id INT PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Koneksi awal gagal: {e}")
        sys.exit(1)

    idx = 1

    print("\n--- MULAI PENGUJIAN ---")
    print(
        "Petunjuk: Coba 'docker stop' atau 'docker network disconnect' pada salah satu node."
    )

    while True:
        try:
            cur = conn.cursor()
            cur.execute(f"INSERT INTO app_db.failover_bench (id) VALUES ({idx})")
            conn.commit()

            print(f"\n[WRITE] Insert ID {idx} ke {current_node['name']} BERHASIL.")

            check_data_consistency(idx)

            idx += 1
            time.sleep(1)

        except mysql.connector.Error as err:
            print(f"\n[FAILOVER] Koneksi terputus ke {current_node['name']}!")
            failover_start = time.time()

            new_conn = None

            while new_conn is None:
                for node in NODES:
                    try:
                        temp_conn = get_connection(node)
                        temp_cur = temp_conn.cursor()
                        temp_cur.execute(
                            f"INSERT INTO app_db.failover_bench (id) VALUES ({idx})"
                        )
                        temp_conn.commit()

                        failover_end = time.time()
                        new_conn = temp_conn
                        current_node = node

                        print(f"[RECOVERED] Failover Selesai.")
                        print(f"Primary Baru: {current_node['name']}")
                        print(f"Downtime: {failover_end - failover_start:.4f} detik")

                        conn = new_conn

                        check_data_consistency(idx)

                        idx += 1
                        break
                    except:
                        pass

                if new_conn is None:
                    print(".", end="", flush=True)
                    time.sleep(0.5)


if __name__ == "__main__":
    run_continuous_failover_test()
