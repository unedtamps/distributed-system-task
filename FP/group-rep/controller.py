import subprocess
import sys
import time

import mysql.connector

DB_CONFIG = {
    "user": "root",
    "password": "rootpassword",
    "auth_plugin": "mysql_native_password",
}

GROUP_NAME = "aaaaaaaa-bbbb-cccc-dddd-eeeeffff0000"
SEEDS = "mysql-node1:33061,mysql-node2:33061,mysql-node3:33061"
DOCKER_NETWORK_NAME = "group-net"

NODES = {
    "node1": {
        "host": "127.0.0.1",
        "port": 3306,
        "internal_host": "mysql-node1",
        "container": "mysql-node1",
    },
    "node2": {
        "host": "127.0.0.1",
        "port": 3307,
        "internal_host": "mysql-node2",
        "container": "mysql-node2",
    },
    "node3": {
        "host": "127.0.0.1",
        "port": 3308,
        "internal_host": "mysql-node3",
        "container": "mysql-node3",
    },
}


def get_db_connection(node_key):
    node = NODES[node_key]
    cfg = DB_CONFIG.copy()
    cfg.update({"host": node["host"], "port": node["port"]})
    return mysql.connector.connect(**cfg)


def check_status():
    success = False
    for key, node in NODES.items():
        try:
            conn = get_db_connection(key)
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

            print(f"Cluster View from {key}:")
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
        except:
            continue

    if not success:
        print("No online nodes found or Cluster is down.")


def start_node(node_key):
    node = NODES[node_key]
    print(f"Starting container {node['container']}...")
    subprocess.run(["docker", "start", node["container"]])


def stop_node(node_key):
    node = NODES[node_key]
    print(f"Stopping container {node['container']}...")
    subprocess.run(["docker", "stop", node["container"]])


def connect_group(node_key):
    node = NODES[node_key]
    print(f"Configuring and Starting Group Replication on {node_key}...")

    try:
        conn = get_db_connection(node_key)
        conn.autocommit = True
        cur = conn.cursor()

        try:
            cur.execute(
                "INSTALL PLUGIN group_replication SONAME 'group_replication.so'"
            )
        except:
            pass

        try:
            cur.execute("STOP GROUP_REPLICATION")
        except:
            pass

        cur.execute(f"SET GLOBAL group_replication_group_name='{GROUP_NAME}'")
        cur.execute(f"SET GLOBAL group_replication_group_seeds='{SEEDS}'")
        cur.execute(
            f"SET GLOBAL group_replication_local_address='{node['internal_host']}:33061'"
        )
        cur.execute("SET GLOBAL group_replication_ip_allowlist='0.0.0.0/0'")
        cur.execute("SET GLOBAL group_replication_single_primary_mode=ON")
        cur.execute("SET GLOBAL group_replication_enforce_update_everywhere_checks=OFF")

        cur.execute("START GROUP_REPLICATION")
        print("Success.")
        conn.close()
    except Exception as e:
        print(f"Failed: {e}")


def disconnect_group(node_key):
    print(f"Stopping Group Replication on {node_key}...")
    try:
        conn = get_db_connection(node_key)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("STOP GROUP_REPLICATION")
        print("Success.")
        conn.close()
    except Exception as e:
        print(f"Failed: {e}")


def net_disconnect(node_key):
    node = NODES[node_key]
    print(f"Disconnecting {node['container']} from docker network...")
    subprocess.run(
        ["docker", "network", "disconnect", DOCKER_NETWORK_NAME, node["container"]]
    )


def net_connect(node_key):
    node = NODES[node_key]
    print(f"Connecting {node['container']} to docker network...")
    subprocess.run(
        ["docker", "network", "connect", DOCKER_NETWORK_NAME, node["container"]]
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python controller.py <action> [node]")
        print("Actions: status, start, stop, join, leave, net_cut, net_join")
        sys.exit(1)

    action = sys.argv[1].lower()

    if action == "status":
        check_status()
    else:
        if len(sys.argv) < 3:
            print("Error: Target node required for this action (node1, node2, node3)")
            sys.exit(1)

        target = sys.argv[2].lower()
        if target not in NODES:
            print("Error: Unknown node")
            sys.exit(1)

        if action == "start":
            start_node(target)
        elif action == "stop":
            stop_node(target)
        elif action == "join":
            connect_group(target)
        elif action == "leave":
            disconnect_group(target)
        elif action == "net_cut":
            net_disconnect(target)
        elif action == "net_join":
            net_connect(target)
        else:
            print("Unknown action")
