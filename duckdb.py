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

print("Initializing DuckDB connection...")
with duckdb.connect("./data/my_duckdb.db") as con:
    try:
        print("Loading PostgreSQL Scanner extension...")
        con.load_extension("./.venv/lib/python3.11/site-packages/duckdb/extensions/v1.1.3/linux_amd64_gcc4/postgres_scanner.duckdb_extension")

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

        for month in range(1, 13):
            print(f"Processing data for month {month} of 2024...")

            # Export data to a CSV file
            export_file = f"./data/month_{month}_2024.csv"
            con.execute(
                f"""
                COPY (
                    SELECT * FROM {source_table}
                    WHERE EXTRACT(MONTH FROM betriebstag) = {month}
                ) TO '{export_file}' (FORMAT CSV, HEADER TRUE);
                    """
            )

            print(f"Month {month}: Data exported to {export_file}")

            # Import data from the CSV file into DuckDB
            con.execute(
                f"""
                COPY {target_table} FROM '{export_file}' (FORMAT CSV, HEADER TRUE);
                """
            )
            
            print(f"Month {month}: Data imported successfully.")

        print("All months processed successfully.")

    except Exception as e:
        print(f"Error during data transfer: {e}")
