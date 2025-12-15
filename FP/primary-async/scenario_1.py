import concurrent.futures
import statistics
import subprocess
import sys
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
    print("Menyiapkan tabel performance_test di Primary...")
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
    print("Setup Selesai.\n")


def check_replica_lag(conn, cursor, target_id, primary_commit_time):
    found = False
    start_poll = time.time()

    while not found:
        conn.commit()
        cursor.execute(
            "SELECT created_at FROM performance_test WHERE id = %s", (target_id,)
        )
        row = cursor.fetchone()

        if row:
            found = True
            arrival_time = time.time()
        else:
            if time.time() - start_poll > 10:
                return 10.0
            time.sleep(0.001)

    lag = arrival_time - primary_commit_time
    return max(0, lag)


def check_row_existence(conn, cursor, target_id):
    conn.commit()
    cursor.execute("SELECT id FROM performance_test WHERE id = %s", (target_id,))
    row = cursor.fetchone()
    return row is not None


def scenario_1_per_row():
    print("Latency Per Row")
    setup_table()

    conn_primary = create_connection("Primary")
    cur_primary = conn_primary.cursor()

    conn_r1 = create_connection("Replica_1")
    cur_r1 = conn_r1.cursor()

    conn_r2 = create_connection("Replica_2")
    cur_r2 = conn_r2.cursor()

    total_rows = 1000
    lags_r1 = []
    lags_r2 = []

    print(f"{'Row':<5} | {'R1 Lag (s)':<12} | {'R2 Lag (s)':<12}")
    print("-" * 35)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    for i in range(1, total_rows + 1):
        cur_primary.execute(
            "INSERT INTO performance_test (id, payload) VALUES (%s, %s)",
            (i, f"Payload-{i}"),
        )
        conn_primary.commit()
        commit_time = time.time()

        f1 = executor.submit(check_replica_lag, conn_r1, cur_r1, i, commit_time)
        f2 = executor.submit(check_replica_lag, conn_r2, cur_r2, i, commit_time)

        l1 = f1.result()
        l2 = f2.result()

        lags_r1.append(l1)
        lags_r2.append(l2)
        if i % 100 == 0 or i == 1:
            print(f"{i:<5} | {l1:.6f}       | {l2:.6f}")

    print("-" * 35)
    print(f"Min Lag R1: {min(lags_r1):.6f} s")
    print(f"Max Lag R1: {max(lags_r2):.6f} s")
    print(f"Min Lag R2: {min(lags_r2):.6f} s")
    print(f"Max Lag R2: {max(lags_r2):.6f} s")
    print(f"Rata-rata Lag R1: {statistics.mean(lags_r1):.6f} s")
    print(f"Rata-rata Lag R2: {statistics.mean(lags_r2):.6f} s")

    cur_primary.close()
    conn_primary.close()
    cur_r1.close()
    conn_r1.close()
    cur_r2.close()
    conn_r2.close()


def scenario_2_bulk():
    print("Visualisasi Eventual Consistency")
    setup_table()

    conn_primary = create_connection("Primary")
    cur_primary = conn_primary.cursor()

    conn_r1 = create_connection("Replica_1")
    cur_r1 = conn_r1.cursor()

    total_rows = 1000

    print(f"Primary Memulai Insert {total_rows} baris...")
    start_write = time.time()

    for i in range(1, total_rows + 1):
        cur_primary.execute(
            "INSERT INTO performance_test (id, payload) VALUES (%s, %s)",
            (i, f"Stream-Data-{i}"),
        )
        conn_primary.commit()

        if i % 200 == 0:
            print(f"   ... Primary: Inserted {i} rows")

    duration_write = time.time() - start_write
    print(
        f"Primary SELESAI Insert {total_rows} baris dalam {duration_write:.4f} detik."
    )
    print("Replica Memeriksa jumlah data yang masuk...\n")
    print(
        f"{'Time (s)':<10} | {'Target Count':<15} | {'Replica Count':<15} | {'Status'}"
    )
    print("-" * 65)
    current_count = 0
    start_monitor = time.time()

    while True:
        conn_r1.commit()
        cur_r1.execute("SELECT COUNT(*) FROM performance_test")
        current_count = cur_r1.fetchone()[0]
        elapsed = time.time() - start_monitor
        if current_count < total_rows:
            status = "LAGGING"
        else:
            status = "SYNCED"

        print(f"{elapsed:<10.4f} | {total_rows:<15} | {current_count:<15} | {status}")

        if current_count >= total_rows:
            break

    print("-" * 65)
    print(f"Total waktu pemulihan konsistensi: {elapsed:.4f} detik.")
    cur_primary.close()
    conn_primary.close()
    cur_r1.close()
    conn_r1.close()


def scenario_3_atomicity_isolation():
    print("Transaction Atomicity Isolation")
    setup_table()

    conn_primary = create_connection("Primary")
    conn_primary.autocommit = False
    cur_primary = conn_primary.cursor()

    conn_r1 = create_connection("Replica_1")
    cur_r1 = conn_r1.cursor()

    row_count = 1000
    print(f"Start Transaksi Insert {row_count} rows (Uncommitted)...")

    for i in range(1, row_count + 1):
        cur_primary.execute(
            "INSERT INTO performance_test (id, payload) VALUES (%s, %s)",
            (i, "Atomicity-Test"),
        )

    conn_r1.commit()
    cur_r1.execute("SELECT COUNT(*) FROM performance_test")
    initial_count = cur_r1.fetchone()[0]
    print(f"Status Replica Sebelum Commit: {initial_count} rows (Harus 0)")

    if initial_count > 0:
        print("GAGAL: Dirty Read terdeteksi! Transaksi bocor sebelum commit.")
        return

    print("Melakukan COMMIT pada Primary...")
    start_commit = time.time()
    conn_primary.commit()

    print("Monitoring Replica untuk lonjakan data (Atomic Jump)...")
    print(f"{'Time (s)':<10} | {'Replica Count':<15} | {'Status'}")
    print("-" * 50)

    while True:
        conn_r1.commit()
        cur_r1.execute("SELECT COUNT(*) FROM performance_test")
        current_count = cur_r1.fetchone()[0]
        elapsed = time.time() - start_commit

        if current_count == 0:
            status = "PENDING (Not Visible)"
        elif current_count < row_count:
            status = "PARTIAL (Atomicity Failed)"
        else:
            status = "COMPLETE (Atomic Success)"

        print(f"{elapsed:<10.4f} | {current_count:<15} | {status}")

        if current_count >= row_count:
            break

    cur_primary.close()
    conn_primary.close()
    cur_r1.close()
    conn_r1.close()


def scenario_4_durability():
    print("SCENARIO 4: Durability Test")
    setup_table()

    conn_primary = create_connection("Primary")
    cur_primary = conn_primary.cursor()

    conn_r1 = create_connection("Replica_1")
    cur_r1 = conn_r1.cursor()

    row_count = 1000
    print(f"Start Insert {row_count} rows...")

    for i in range(1, row_count + 1):
        cur_primary.execute(
            "INSERT INTO performance_test (id, payload) VALUES (%s, %s)",
            (i, f"Durability-Data-{i}"),
        )
        conn_primary.commit()

    print("SHUTDOWN PRIMARY NODE...")
    subprocess.run(["docker", "kill", "mysql-primary"], capture_output=True)

    print("Waiting 5 seconds...")
    time.sleep(5)

    print("Checking Replica Count...")
    try:
        conn_r1.commit()
        cur_r1.execute("SELECT COUNT(*) FROM performance_test")
        replica_count = cur_r1.fetchone()[0]

        print(f"Replica Count: {replica_count}")

        if replica_count < row_count:
            loss = row_count - replica_count
            print(f"RESULT: DURABILITY FAILED. Data Loss: {loss} rows.")
        else:
            print("RESULT: FULLY REPLICATED. No Data Loss.")

    except mysql.connector.Error as err:
        print(f"Error reading replica: {err}")

    cur_r1.close()
    conn_r1.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Gunakan argumen: 1, 2, 3, 4")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "1":
        scenario_1_per_row()
    elif mode == "2":
        scenario_2_bulk()
    elif mode == "3":
        scenario_3_atomicity_isolation()
    elif mode == "4":
        scenario_4_durability()
    else:
        print("Pilihan tidak valid.")
