import subprocess
import sys
import time

import mysql.connector

DB_CONFIG = {
    "user": "root",
    "password": "rootpassword",
    "auth_plugin": "mysql_native_password",
}

DOCKER_NETWORK_NAME = "group-net"

NODES = {
    "node1": {"port": 3306, "container": "mysql-node1", "internal_host": "mysql-node1"},
    "node2": {"port": 3307, "container": "mysql-node2", "internal_host": "mysql-node2"},
    "node3": {"port": 3308, "container": "mysql-node3", "internal_host": "mysql-node3"},
}


def get_connection(port):
    return mysql.connector.connect(host="127.0.0.1", port=port, **DB_CONFIG)


def get_primary_node_key():
    print("Searching for Primary Node...", end=" ")
    found_primary_host = None

    for key, node in NODES.items():
        try:
            conn = get_connection(node["port"])
            cur = conn.cursor(dictionary=True)
            cur.execute(
                "SELECT MEMBER_HOST FROM performance_schema.replication_group_members "
                "WHERE MEMBER_ROLE='PRIMARY' AND MEMBER_STATE='ONLINE'"
            )
            row = cur.fetchone()
            conn.close()
            if row:
                found_primary_host = row["MEMBER_HOST"]
                break
        except:
            continue

    if not found_primary_host:
        print("No primary detected. Cluster may be down.")
        sys.exit(1)

    target_key = None
    for key, node in NODES.items():
        if node["internal_host"] == found_primary_host:
            target_key = key
            break

    print(f"Primary: {target_key.upper()} ({found_primary_host})")
    return target_key


def toggle_network(container_name, action):
    print(f"Network {action} for {container_name}...")
    subprocess.run(
        ["docker", "network", action, DOCKER_NETWORK_NAME, container_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def verify_failover(disconnected_node_key):
    print("Checking cluster status from surviving node...")
    alive_node_key = [k for k in NODES if k != disconnected_node_key][0]
    alive_node = NODES[alive_node_key]

    try:
        conn = get_connection(alive_node["port"])
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT MEMBER_HOST, MEMBER_ROLE, MEMBER_STATE "
            "FROM performance_schema.replication_group_members "
            "WHERE MEMBER_STATE='ONLINE'"
        )
        rows = cur.fetchall()
        print(f"[View from {alive_node_key}]")
        for row in rows:
            print(
                f"- {row['MEMBER_HOST']}: {row['MEMBER_ROLE']} ({row['MEMBER_STATE']})"
            )
        conn.close()
    except:
        print("Failover check failed.")


def run_scenario():
    print("SCENARIO 3: SPLIT-BRAIN TEST")

    primary_key = get_primary_node_key()
    primary_node = NODES[primary_key]

    try:
        conn = get_connection(primary_node["port"])
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS test_db")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS test_db.split_test (id INT PRIMARY KEY, msg VARCHAR(50))"
        )
        cur.execute("DELETE FROM test_db.split_test")
        conn.close()
        print("Test table ready.")
    except Exception as e:
        print(f"Setup error: {e}")

    print(f"Disconnecting primary node: {primary_key}...")
    toggle_network(primary_node["container"], "disconnect")

    print("Waiting 10 seconds for cluster to detect failure...")
    time.sleep(10)

    verify_failover(primary_key)

    print(f"Attempting local write inside container {primary_key} (network bypass)...")
    start_time = time.time()

    cmd = [
        "docker",
        "exec",
        primary_node["container"],
        "mysql",
        "-uroot",
        "-prootpassword",
        "-e",
        "INSERT INTO test_db.split_test VALUES (199, 'Illegal Data');",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    duration = time.time() - start_time

    if result.returncode != 0:
        print(f"Write rejected (Duration: {duration:.2f}s)")
        print(f"MySQL Error:\n{result.stderr.strip()}")
    else:
        print("Write succeeded. This indicates a split-brain condition.")

    print("Restoring network...")
    toggle_network(primary_node["container"], "connect")
    print("Recovery complete. Allow time for re-sync.")


if __name__ == "__main__":
    run_scenario()
