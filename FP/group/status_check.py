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


def check_cluster_status():
    print("\n--- Checking Cluster Status (Failover Mode) ---")

    status_found = False

    # Kita loop setiap node yang ada di daftar config
    for node in NODES:
        cfg = DB_CONFIG.copy()
        cfg.update({"host": node["host"], "port": node["port"]})

        try:
            print(
                f"Attempting to connect to {node['name']} (Port {node['port']})...",
                end=" ",
            )
            conn = mysql.connector.connect(**cfg)

            # Jika baris ini tereksekusi, berarti koneksi berhasil!
            print("SUCCESS ✅")

            cur = conn.cursor(dictionary=True)
            cur.execute(
                "SELECT MEMBER_HOST, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members"
            )
            rows = cur.fetchall()

            print(f"\n[View from {node['name']}]")
            print(f"{'MEMBER_HOST':<15} | {'MEMBER_STATE':<12} | {'ROLE':<10}")
            print("-" * 45)
            for row in rows:
                print(
                    f"{row['MEMBER_HOST']:<15} | {row['MEMBER_STATE']:<12} | {row['MEMBER_ROLE']:<10}"
                )

            conn.close()
            status_found = True

            # Kita break loop karena kita sudah dapat status cluster dari salah satu member.
            # (Dalam Group Replication, semua member punya view yang sama terhadap cluster)
            break

        except mysql.connector.Error:
            # Jika gagal connect, print pesan error singkat dan lanjut ke node berikutnya
            print("UNREACHABLE ❌")
            continue

    if not status_found:
        print("\n[CRITICAL] All nodes are unreachable! Cluster is DOWN.")


if __name__ == "__main__":
    check_cluster_status()
