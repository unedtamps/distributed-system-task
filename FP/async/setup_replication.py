import time

import mysql.connector

DB_USER = "root"
DB_PASS = "rootpassword"

PRIMARY_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": DB_USER,
    "password": DB_PASS,
}
REPLICA1_CONFIG = {
    "host": "127.0.0.1",
    "port": 3307,
    "user": DB_USER,
    "password": DB_PASS,
}
REPLICA2_CONFIG = {
    "host": "127.0.0.1",
    "port": 3308,
    "user": DB_USER,
    "password": DB_PASS,
}


def run_query(config, query, params=None):
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        print(f"SUCCESS Executed on port {config['port']}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"ERROR Port {config['port']}: {e}")
        return False


def setup_primary():
    print("\nSetting up Primary")
    queries = [
        "CREATE USER IF NOT EXISTS 'repl_user'@'%' IDENTIFIED BY 'password';",
        "GRANT REPLICATION SLAVE ON *.* TO 'repl_user'@'%';",
        "FLUSH PRIVILEGES;",
    ]
    for q in queries:
        run_query(PRIMARY_CONFIG, q)


def setup_replica(replica_config, replica_name):
    print(f"\nSetting up {replica_name}")

    run_query(replica_config, "STOP REPLICA;")
    query_change_source = """
    CHANGE REPLICATION SOURCE TO
      SOURCE_HOST='mysql-primary',
      SOURCE_USER='repl_user',
      SOURCE_PASSWORD='password',
      SOURCE_SSL=0,
      GET_SOURCE_PUBLIC_KEY=1;
    """
    run_query(replica_config, query_change_source)
    run_query(replica_config, "START REPLICA;")
    try:
        conn = mysql.connector.connect(**replica_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW REPLICA STATUS")
        status = cursor.fetchone()
        if status:
            print(
                f"Status: IO_Running={status['Replica_IO_Running']}, SQL_Running={status['Replica_SQL_Running']}"
            )
        conn.close()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    print("Pastikan Docker Compose sudah UP. start 2 seconds ...")
    time.sleep(2)
    setup_primary()
    setup_replica(REPLICA1_CONFIG, "Replica 1")
    setup_replica(REPLICA2_CONFIG, "Replica 2")

    print("\nSetup Selesai!")
