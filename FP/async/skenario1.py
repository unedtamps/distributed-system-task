import concurrent.futures
import statistics
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
    cursor.execute("DROP TABLE IF EXISTS performance_test")
    cursor.execute(
        """
        CREATE TABLE performance_test (
            id INT PRIMARY KEY,
            payload VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    conn.commit()
    conn.close()
    print("Tabel 'performance_test' siap.\n")


def check_replica(cursor, conn, node_name, target_id, write_time):
    loop_count = 0
    found = False
    start_poll = time.time()

    while not found:
        conn.commit()
        cursor.execute(
            "SELECT created_at FROM performance_test WHERE id = %s", (target_id,)
        )
        result = cursor.fetchone()
        if result:
            found = True
            end_poll = time.time() - loop_count * 0.001
        else:
            time.sleep(0.001)
            loop_count += 1

            if time.time() - start_poll > 5:
                print(f"  [TIMEOUT] {node_name} tidak menerima data dalam 10s!")
                return 10.0

    lag = end_poll - write_time
    return max(0, lag)


def run_scenario():
    setup_table()

    conn_primary = create_connection("Primary")
    conn_r1 = create_connection("Replica_1")
    conn_r2 = create_connection("Replica_2")

    cur_primary = conn_primary.cursor()
    cur_r1 = conn_r1.cursor()
    cur_r2 = conn_r2.cursor()

    total_rows = 1000
    lags_r1 = []
    lags_r2 = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    print(f"Mulai Insert {total_rows} Rows & Ukur Lag")
    print(f"{'ID':<6} | {'R1 Lag (s)':<12} | {'R2 Lag (s)':<12} | {'Status'}")
    print("-" * 45)

    for i in range(1, total_rows + 1):
        payload = f"Data-Row-{i}"

        start_write = time.time()
        cur_primary.execute(
            "INSERT INTO performance_test (id, payload) VALUES (%s, %s)", (i, payload)
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

        lags_r1.append(lag1)
        lags_r2.append(lag2)

        if i % 100 == 0 or i == 1:
            status = "SYNCED" if (lag1 < 0.01 and lag2 < 0.01) else "LAGGING"
            print(f"{i:<6} | {lag1:.6f}     | {lag2:.6f}     | {status}")

    cur_primary.close()
    cur_r1.close()
    cur_r2.close()

    print("\nHasil Observasi")
    print(f"Rata-rata Lag Replica 1: {statistics.mean(lags_r1):.6f} detik")
    print(f"Rata-rata Lag Replica 2: {statistics.mean(lags_r2):.6f} detik")
    print(f"Min Lag Replica 1: {min(lags_r1):.6f} detik")
    print(f"Min Lag Replica 2: {min(lags_r2):.6f} detik")
    print(f"Max Lag Replica 1: {max(lags_r1):.6f} detik")
    print(f"Max Lag Replica 2: {max(lags_r2):.6f} detik")


if __name__ == "__main__":
    run_scenario()
