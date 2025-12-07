import time

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
        "internal_host": "mysql-node1",
        "is_bootstrap": True,
    },
    {
        "name": "Node 2",
        "host": "127.0.0.1",
        "port": 3307,
        "internal_host": "mysql-node2",
        "is_bootstrap": False,
    },
    {
        "name": "Node 3",
        "host": "127.0.0.1",
        "port": 3308,
        "internal_host": "mysql-node3",
        "is_bootstrap": False,
    },
]

GROUP_NAME = "aaaaaaaa-bbbb-cccc-dddd-eeeeffff0000"
SEEDS = "mysql-node1:33061,mysql-node2:33061,mysql-node3:33061"


def run_sql(config, sql):
    try:
        conn = mysql.connector.connect(**config)
        cur = conn.cursor()
        sql = sql.strip().rstrip(";")

        # Disable Binary Log untuk session ini agar tidak menambah GTID (Opsional tapi bagus)
        cur.execute("SET sql_log_bin = 0")

        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        return True
    except mysql.connector.Error as err:
        if err.errno == 3098:
            pass
        elif "already a member" in str(err) or "is already running" in str(err):
            print(f"   [INFO] Group Replication already running.")
        else:
            print(f"   [ERROR] {err}")
        return False


def configure_node(node):
    print(f"--- Configuring {node['name']} ---")
    cfg = DB_CONFIG.copy()
    cfg.update({"host": node["host"], "port": node["port"]})

    # 1. Install Plugin (Idempotent)
    run_sql(cfg, "INSTALL PLUGIN group_replication SONAME 'group_replication.so'")

    # 2. Setup User Replikasi (Akan ter-log di binlog? Kita sudah disable sql_log_bin di fungsi run_sql)
    print("   Creating replication user...")
    run_sql(cfg, "CREATE USER IF NOT EXISTS 'repl_user'@'%' IDENTIFIED BY 'password'")
    run_sql(
        cfg,
        "GRANT REPLICATION SLAVE, GROUP_REPLICATION_ADMIN ON *.* TO 'repl_user'@'%' WITH GRANT OPTION",
    )
    run_sql(cfg, "FLUSH PRIVILEGES")

    # 3. Setup Channel
    run_sql(cfg, "STOP GROUP_REPLICATION")

    # KITA TAMBAHKAN: SOURCE_SSL=0, GET_SOURCE_PUBLIC_KEY=1
    # query_recovery = """
    #     CHANGE REPLICATION SOURCE TO
    #     SOURCE_USER='repl_user',
    #     SOURCE_PASSWORD='password',
    #     SOURCE_SSL=0,
    #     GET_SOURCE_PUBLIC_KEY=1
    #     FOR CHANNEL 'group_replication_recovery'
    # """
    # run_sql(cfg, query_recovery)
    run_sql(
        cfg,
        f"CHANGE REPLICATION SOURCE TO SOURCE_USER='repl_user', SOURCE_PASSWORD='password' FOR CHANNEL 'group_replication_recovery'",
    )

    # 4. Config Network
    print("   Forcing Network Config...")
    run_sql(cfg, f"SET GLOBAL group_replication_group_name='{GROUP_NAME}'")
    run_sql(cfg, f"SET GLOBAL group_replication_group_seeds='{SEEDS}'")
    run_sql(
        cfg,
        f"SET GLOBAL group_replication_local_address='{node['internal_host']}:33061'",
    )
    run_sql(cfg, "SET GLOBAL group_replication_ip_allowlist='0.0.0.0/0'")
    run_sql(cfg, "SET GLOBAL group_replication_single_primary_mode=ON")
    run_sql(cfg, "SET GLOBAL group_replication_enforce_update_everywhere_checks=OFF")

    # 5. THE MAGIC FIX: RESET MASTER
    # Hapus semua GTID history sebelum start.
    # Node 1 (Bootstrap) sebaiknya jangan di-reset jika dia sudah hidup dan punya data penting.
    # Tapi karena ini setup awal, reset semua tidak masalah.
    if not node["is_bootstrap"]:
        print("   Reseting Master GTID (Clean Slate)...")
        run_sql(cfg, "RESET MASTER")

    # 6. Start Group
    if node["is_bootstrap"]:
        print("   BOOTSTRAPPING GROUP on Node 1...")
        run_sql(cfg, "SET GLOBAL group_replication_bootstrap_group=ON")
        run_sql(cfg, "START GROUP_REPLICATION")
        run_sql(cfg, "SET GLOBAL group_replication_bootstrap_group=OFF")
    else:
        print(f"   Joining Group on {node['name']}...")
        run_sql(cfg, "START GROUP_REPLICATION")


def check_cluster_status():
    print("\n--- Checking Cluster Status ---")
    cfg = DB_CONFIG.copy()
    cfg.update({"host": "127.0.0.1", "port": 3306})
    try:
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT MEMBER_HOST, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members"
        )
        rows = cur.fetchall()
        print(f"{'MEMBER_HOST':<15} | {'MEMBER_STATE':<12} | {'ROLE':<10}")
        print("-" * 45)
        for row in rows:
            print(
                f"{row['MEMBER_HOST']:<15} | {row['MEMBER_STATE']:<12} | {row['MEMBER_ROLE']:<10}"
            )
        conn.close()
    except Exception as e:
        print(f"Status check failed: {e}")


if __name__ == "__main__":
    print("Applying Corrected Fixes...")
    for node in NODES:
        configure_node(node)
        time.sleep(5)

    print("\nSetup Complete! Waiting for sync...")
    time.sleep(5)
    check_cluster_status()
