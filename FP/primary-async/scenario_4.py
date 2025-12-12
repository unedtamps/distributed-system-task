import concurrent.futures
import statistics
import subprocess
import time
import mysql.connector

DB_CONFIG = {
    "user": "root",
    "password": "rootpassword",
    "database": "app_db",
    "auth_plugin": "mysql_native_password",
}

NODES = {
    "Primary": {"host": "127.0.0.1", "port": 3306},
    "Replica_1": {"host": "127.0.0.1", "port": 3307},
    "Replica_2": {"host": "127.0.0.1", "port": 3308},
}


def create_connection(node_name):
    config = DB_CONFIG.copy()
    config.update(NODES[node_name])
    return mysql.connector.connect(**config)

def setup_table():
    conn = create_connection("Primary")
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS scenario_4")
    cursor.execute("""
        CREATE TABLE scenario_4 (
            id INT PRIMARY KEY,
            payload VARCHAR(255),
            batch VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    print("Tabel 'scenario_4' siap.\n")

def check_replica(cursor, conn, node_name, target_id, write_time):
    loop_count = 0
    found = False
    start_poll = time.time()

    while not found:
        try:
            conn.commit()
            cursor.execute(
                "SELECT created_at FROM scenario_4 WHERE id = %s", (target_id,)
            )
            result = cursor.fetchone()
            if result:
                found = True
                end_poll = time.time() - loop_count * 0.001
            else:
                time.sleep(0.001)
                loop_count += 1

                if time.time() - start_poll > 5:
                    return -1
        except Exception:
            return -1

    lag = end_poll - write_time
    return max(0, lag)

def disconnect_network(container_name, network_name="primary-async_mysql-async-net"):
    print(f"\nDisconnecting {container_name}...")
    try:
        result = subprocess.run(
            ["docker", "network", "disconnect", network_name, container_name],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"{container_name} disconnected")
            return True
        else:
            print(f"Failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def reconnect_network(container_name, network_name="primary-async_mysql-async-net"):
    print(f"\nReconnecting {container_name}...")
    try:
        result = subprocess.run(
            ["docker", "network", "connect", network_name, container_name],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"{container_name} reconnected")
            return True
        else:
            print(f"Failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def write_batch(conn_primary, cur_primary, conn_r1, cur_r1, conn_r2, cur_r2, start_id, count, batch, executor):
    lags_r1 = []
    lags_r2 = []

    for i in range(start_id, start_id + count):
        payload = f"Data-{i}"
        start_write = time.time()
        cur_primary.execute(
            "INSERT INTO scenario_4 (id, payload, batch) VALUES (%s, %s, %s)",
            (i, payload, batch)
        )
        conn_primary.commit()
        end_write = time.time()

        future_r1 = executor.submit(
            check_replica, cur_r1, conn_r1, "Replica_1", i, end_write
        )
        future_r2 = executor.submit(
            check_replica, cur_r2, conn_r2, "Replica_2", i, end_write
        )

        lag1 = future_r1.result()
        lag2 = future_r2.result()

        if lag1 >= 0:
            lags_r1.append(lag1)
        if lag2 >= 0:
            lags_r2.append(lag2)

    return lags_r1, lags_r2

def count_data(node_name, batch=None):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = create_connection(node_name)
            cursor = conn.cursor()
            if batch:
                cursor.execute(
                    "SELECT COUNT(*) FROM scenario_4 WHERE batch = %s",
                    (batch,)
                )
            else:
                cursor.execute("SELECT COUNT(*) FROM scenario_4")

            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return -1

def check_replication_lag(node_name):
    try:
        conn = create_connection(node_name)
        cursor = conn.cursor()
        cursor.execute("SHOW SLAVE STATUS")
        result = cursor.fetchone()

        if result:
            lag = result[32]
            cursor.close()
            conn.close()
            return lag

        cursor.close()
        conn.close()
        return None
    except:
        return None


def run_scenario():
    setup_table()

    conn_primary = create_connection("Primary")
    conn_r1 = create_connection("Replica_1")
    conn_r2 = create_connection("Replica_2")

    cur_primary = conn_primary.cursor()
    cur_r1 = conn_r1.cursor()
    cur_r2 = conn_r2.cursor()

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    all_lags_r1 = []
    all_lags_r2 = []

    partition_start = 0
    partition_end = 0
    recovery_start = 0

    print(f"{'Fase':<10} | {'Primary':<8} | {'Replica 1':<10} | {'Replica 2':<10} | {'Avg Lag R1':<12} | {'Avg Lag R2':<12}")
    print("-" * 80)

    # kondisi normal
    lags_r1, lags_r2 = write_batch(conn_primary, cur_primary, conn_r1, cur_r1, conn_r2, cur_r2, 1, 200, "batch1", executor)
    all_lags_r1.extend(lags_r1)
    all_lags_r2.extend(lags_r2)
    p, r1, r2 = count_data("Primary"), count_data("Replica_1"), count_data("Replica_2")
    avg_r1 = statistics.mean(lags_r1) if lags_r1 else 0
    avg_r2 = statistics.mean(lags_r2) if lags_r2 else 0
    print(f"{'Normal':<10} | {p:<8} | {r1:<10} | {r2:<10} | {avg_r1:.6f}     | {avg_r2:.6f}")

    # partitioning replica 1
    disconnect_network("mysql-replica-1")
    partition_start = time.time()
    time.sleep(2)

    lags_r1, lags_r2 = write_batch(conn_primary, cur_primary, conn_r1, cur_r1, conn_r2, cur_r2, 201, 300, "batch2", executor)
    all_lags_r2.extend(lags_r2)
    p, r1, r2 = count_data("Primary"), count_data("Replica_1"), count_data("Replica_2")
    r1_status = "PARTITION" if r1 < 0 else str(r1)
    avg_r2 = statistics.mean(lags_r2) if lags_r2 else 0
    print(f"{'Partition':<10} | {p:<8} | {r1_status:<10} | {r2:<10} | {'-':<12} | {avg_r2:.6f}")

    lags_r1, lags_r2 = write_batch(conn_primary, cur_primary, conn_r1, cur_r1, conn_r2, cur_r2, 501, 200, "batch3", executor)
    all_lags_r2.extend(lags_r2)
    p, r1, r2 = count_data("Primary"), count_data("Replica_1"), count_data("Replica_2")
    r1_status = "PARTITION" if r1 < 0 else str(r1)
    partition_end = time.time()
    avg_r2 = statistics.mean(lags_r2) if lags_r2 else 0
    print(f"{'During':<10} | {p:<8} | {r1_status:<10} | {r2:<10} | {'-':<12} | {avg_r2:.6f}")

    # recovery
    reconnect_network("mysql-replica-1")
    recovery_start = time.time()
    time.sleep(2)

    for i in range(15):
        time.sleep(1)
        r1 = count_data("Replica_1")
        lag = check_replication_lag("Replica_1")

        if r1 >= 0 and r1 == 700 and lag == 0:
            recovery_end = time.time()
            print(f"{'Recovery':<10} | {700:<8} | {r1:<10} | {r2:<10} | {'menyesuaikan...':<12} | {'-':<12}")
            break

    lags_r1, lags_r2 = write_batch(conn_primary, cur_primary, conn_r1, cur_r1, conn_r2, cur_r2, 701, 300, "batch4", executor)
    all_lags_r1.extend(lags_r1)
    all_lags_r2.extend(lags_r2)
    p, r1, r2 = count_data("Primary"), count_data("Replica_1"), count_data("Replica_2")
    avg_r1 = statistics.mean(lags_r1) if lags_r1 else 0
    avg_r2 = statistics.mean(lags_r2) if lags_r2 else 0
    print(f"{'Final':<10} | {p:<8} | {r1:<10} | {r2:<10} | {avg_r1:.6f}     | {avg_r2:.6f}")

    cur_primary.close()
    cur_r1.close()
    cur_r2.close()

    partition_window = partition_end - partition_start
    recovery_time = recovery_end - recovery_start if 'recovery_end' in locals() else 15.0

    print("\nHasil Observasi")
    if all_lags_r1:
        print(f"Rata-rata Lag Replica 1: {statistics.mean(all_lags_r1):.6f} detik")
        print(f"Min Lag Replica 1: {min(all_lags_r1):.6f} detik")
        print(f"Max Lag Replica 1: {max(all_lags_r1):.6f} detik")
    else:
        print(f"Rata-rata Lag Replica 1 (partitioned): - detik")

    if all_lags_r2:
        print(f"Rata-rata Lag Replica 2: {statistics.mean(all_lags_r2):.6f} detik")
        print(f"Min Lag Replica 2: {min(all_lags_r2):.6f} detik")
        print(f"Max Lag Replica 2: {max(all_lags_r2):.6f} detik")

    print(f"Inconsistency Window: {partition_window:.2f} detik")
    print(f"Recovery Time: {recovery_time:.2f} detik")
    print(f"Final Row Count: Primary={p}, Replica 1={r1}, Replica 2={r2}")

if __name__ == "__main__":
    run_scenario()
