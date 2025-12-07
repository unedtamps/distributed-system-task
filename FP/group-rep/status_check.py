import sys

import mysql.connector

DB_CONFIG = {
    "user": "root",
    "password": "rootpassword",
    "auth_plugin": "mysql_native_password",
}

NODES = [
    {
        "name": "Node 1",
        "host": "127.0.0.1",
        "port": 3306,
    },
    {
        "name": "Node 2",
        "host": "127.0.0.1",
        "port": 3307,
    },
    {
        "name": "Node 3",
        "host": "127.0.0.1",
        "port": 3308,
    },
]


def check_cluster_status():
    success = False

    for node in NODES:
        cfg = DB_CONFIG.copy()
        cfg.update({"host": node["host"], "port": node["port"]})

        try:
            conn = mysql.connector.connect(**cfg)

            cur = conn.cursor(dictionary=True)
            cur.execute(
                "SELECT MEMBER_HOST, MEMBER_STATE, MEMBER_ROLE "
                "FROM performance_schema.replication_group_members "
                "WHERE MEMBER_STATE = 'ONLINE'"
            )
            rows = cur.fetchall()

            if not rows:
                conn.close()
                continue

            print(f"Connected to {node['name']} (Port {node['port']})")
            print(f"\nCluster View (ONLINE MEMBERS ONLY):")
            print("-" * 50)
            print(f"{'MEMBER_HOST':<20} | {'STATE':<15} | {'ROLE':<10}")
            print("-" * 50)

            for row in rows:
                print(
                    f"{row['MEMBER_HOST']:<20} | {row['MEMBER_STATE']:<15} | {row['MEMBER_ROLE']:<10}"
                )

            print("-" * 50)
            conn.close()
            success = True
            break

        except mysql.connector.Error:
            continue

    if not success:
        print("No online nodes found.")


if __name__ == "__main__":
    check_cluster_status()
