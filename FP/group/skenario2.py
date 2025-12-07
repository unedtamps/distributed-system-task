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


def get_connection(node):
    return mysql.connector.connect(
        **DB_CONFIG, host=node["host"], port=node["port"], connect_timeout=1
    )


def find_initial_primary():
    for node in NODES:
        try:
            conn = get_connection(node)
            cur = conn.cursor(dictionary=True)
            cur.execute(
                "SELECT * FROM performance_schema.replication_group_members WHERE MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'"
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


def run_continuous_failover_test():
    current_node = find_initial_primary()

    if not current_node:
        print("Cluster Down")
        sys.exit(1)

    print(f"Connected to Primary {current_node['name']}")

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
        print(f"Connection Error {e}")
        sys.exit(1)

    idx = 1

    while True:
        try:
            cur = conn.cursor()
            cur.execute(f"INSERT INTO app_db.failover_bench (id) VALUES ({idx})")
            conn.commit()

            print(f"Insert ID {idx} success to {current_node['name']}")
            idx += 1
            time.sleep(0.5)

        except mysql.connector.Error as err:
            print(f"\nConnection lost to {current_node['name']}")
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

                        print(f"\nFailover Complete")
                        print(f"New Primary {current_node['name']}")
                        print(
                            f"Total Downtime {failover_end - failover_start:.4f} seconds\n"
                        )

                        conn = new_conn
                        idx += 1
                        break
                    except:
                        pass

                if new_conn is None:
                    time.sleep(0.1)


if __name__ == "__main__":
    run_continuous_failover_test()
