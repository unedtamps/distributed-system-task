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

TOTAL_ROWS = 3000
BURST_SIZE = 400
THROTTLE_THRESHOLD = 0.008
RELAX_THRESHOLD = 0.004
THROTTLE_STEP = 0.05  
MAX_THROTTLE_SLEEP = 0.5  
TABLE_NAME = "burst_throttle"


def create_connection(node_name):
    config = DB_CONFIG.copy()
    config.update(NODES[node_name])
    return mysql.connector.connect(**config)


def setup_table():
    conn = create_connection("Primary")
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    cursor.execute(
        f"""
        CREATE TABLE {TABLE_NAME} (
            id INT PRIMARY KEY,
            payload VARCHAR(255),
            batch INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    conn.commit()
    conn.close()
    print(f"Tabel '{TABLE_NAME}' siap.\n")


def check_replica(cursor, conn, node_name, target_id, write_time):
    loop_count = 0
    start_poll = time.time()

    while True:
        try:
            conn.commit()
            cursor.execute(
                f"SELECT created_at FROM {TABLE_NAME} WHERE id = %s",
                (target_id,),
            )
            result = cursor.fetchone()
            if result:
                end_poll = time.time() - loop_count * 0.001
                return max(0, end_poll - write_time)

            time.sleep(0.001)
            loop_count += 1

            if time.time() - start_poll > 5:
                return 5.0
        except Exception:
            return 5.0


def write_burst(
    conn_primary,
    cur_primary,
    conn_r1,
    cur_r1,
    conn_r2,
    cur_r2,
    start_id,
    end_id,
    batch,
    executor,
):
    batch_lags_r1 = []
    batch_lags_r2 = []

    for row_id in range(start_id, end_id + 1):
        payload = f"Burst-{row_id}"
        cur_primary.execute(
            f"INSERT INTO {TABLE_NAME} (id, payload, batch) VALUES (%s, %s, %s)",
            (row_id, payload, batch),
        )
        conn_primary.commit()
        write_time = time.time()

        future_r1 = executor.submit(
            check_replica, cur_r1, conn_r1, "Replica_1", row_id, write_time
        )
        future_r2 = executor.submit(
            check_replica, cur_r2, conn_r2, "Replica_2", row_id, write_time
        )

        batch_lags_r1.append(future_r1.result())
        batch_lags_r2.append(future_r2.result())

    return batch_lags_r1, batch_lags_r2


def print_summary(lags_r1, lags_r2, throttle_values):
    print("\nHasil Observasi")
    if lags_r1:
        print(f"Rata-rata Lag Replica 1 : {statistics.mean(lags_r1):.6f} detik")
        print(f"Min Lag Replica 1      : {min(lags_r1):.6f} detik")
        print(f"Max Lag Replica 1      : {max(lags_r1):.6f} detik")
    if lags_r2:
        print(f"Rata-rata Lag Replica 2 : {statistics.mean(lags_r2):.6f} detik")
        print(f"Min Lag Replica 2      : {min(lags_r2):.6f} detik")
        print(f"Max Lag Replica 2      : {max(lags_r2):.6f} detik")

    if throttle_values:
        print(
            f"Throttle Delay (min/avg/max): {min(throttle_values):.3f}s / "
            f"{statistics.mean(throttle_values):.3f}s / {max(throttle_values):.3f}s"
        )
    print(f"Total Rows Dikirim : {TOTAL_ROWS}")


def run_scenario():
    setup_table()

    conn_primary = create_connection("Primary")
    conn_r1 = create_connection("Replica_1")
    conn_r2 = create_connection("Replica_2")

    cur_primary = conn_primary.cursor()
    cur_r1 = conn_r1.cursor()
    cur_r2 = conn_r2.cursor()

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    all_lags_r1 = []
    all_lags_r2 = []
    throttle_delay = 0.0
    throttle_history = []

    print(
        f"{'Batch':<6} | {'Rows':<6} | {'Avg Lag R1':<12} | "
        f"{'Avg Lag R2':<12} | {'Action':<13} | {'Next Delay (s)':<14}"
    )
    print("-" * 80)

    total_batches = (TOTAL_ROWS + BURST_SIZE - 1) // BURST_SIZE

    for batch_idx in range(total_batches):
        start_id = batch_idx * BURST_SIZE + 1
        end_id = min(start_id + BURST_SIZE - 1, TOTAL_ROWS)
        rows_in_batch = end_id - start_id + 1

        batch_lags_r1, batch_lags_r2 = write_burst(
            conn_primary,
            cur_primary,
            conn_r1,
            cur_r1,
            conn_r2,
            cur_r2,
            start_id,
            end_id,
            batch_idx + 1,
            executor,
        )

        all_lags_r1.extend(batch_lags_r1)
        all_lags_r2.extend(batch_lags_r2)

        avg_r1 = statistics.mean(batch_lags_r1) if batch_lags_r1 else 0
        avg_r2 = statistics.mean(batch_lags_r2) if batch_lags_r2 else 0
        dominant_avg = max(avg_r1, avg_r2)

        action = "STABLE"
        new_delay = throttle_delay
        if dominant_avg > THROTTLE_THRESHOLD and throttle_delay < MAX_THROTTLE_SLEEP:
            new_delay = min(MAX_THROTTLE_SLEEP, throttle_delay + THROTTLE_STEP)
            action = "THROTTLE_UP"
        elif dominant_avg < RELAX_THRESHOLD and throttle_delay > 0:
            new_delay = max(0.0, throttle_delay - THROTTLE_STEP)
            action = "THROTTLE_DOWN"

        throttle_history.append(new_delay)

        print(
            f"{batch_idx + 1:<6} | {rows_in_batch:<6} | "
            f"{avg_r1:.6f}    | {avg_r2:.6f}    | {action:<13} | {new_delay:.3f}"
        )

        throttle_delay = new_delay
        if throttle_delay > 0:
            time.sleep(throttle_delay)

    cur_primary.close()
    cur_r1.close()
    cur_r2.close()

    print_summary(all_lags_r1, all_lags_r2, throttle_history)


if __name__ == "__main__":
    run_scenario()
