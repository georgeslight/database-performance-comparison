import io
import os
from dotenv import load_dotenv
import duckdb

load_dotenv()

POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT'))
}

try:
    print("Initializing DuckDB connection...")
    con = duckdb.connect("my_duckdb.db")
    print("Loading PostgreSQL Scanner extension...")
    con.load_extension("./.venv/lib/python3.11/site-packages/duckdb/extensions/v1.1.3/linux_amd64_gcc4/postgres_scanner.duckdb_extension")
    con.execute("SET memory_limit='32GB';") 
    print("Attaching PostgreSQL database to DuckDB...")
    con.execute(
            f"""
            ATTACH 'dbname={POSTGRES_CONFIG['dbname']}
                user={POSTGRES_CONFIG['user']}
                host={POSTGRES_CONFIG['host']}
                port={POSTGRES_CONFIG['port']}
                password={POSTGRES_CONFIG['password']}'
            AS pg (TYPE POSTGRES, READ_ONLY);
        """
        )

    source_table = 'pg.qdaba.puenkt'
    target_table = 'qdaba.puenkt'

    # Iterate over each month
    for month in range(4, 5):

        print("Extracting valid days.")
        valid_days = con.execute(
            f"""
            SELECT DISTINCT EXTRACT(DAY FROM betriebstag) AS day
            FROM {source_table}
            WHERE EXTRACT(MONTH FROM betriebstag) = {month}
            ORDER BY day
            """
        ).fetchall()

        # Flatten the list of tuples
        valid_days = [int(day[0]) for day in valid_days]

        # Process each day of the month
        for day in valid_days:
            print(f"Processing data for {month}/{day}/2024...")

            con.execute(
                f"""
                INSERT INTO {target_table}
                FROM {source_table}
                WHERE betriebstag = '2014-{month}-{day}';
                """
            )
            print(f"Day {month}/{day}: Data imported successfully.")
    print("All data processed successfully.")
    con.close()

except Exception as e:
    print(f"Error during data transfer: {e}")
