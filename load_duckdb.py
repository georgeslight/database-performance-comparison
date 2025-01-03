import os
from dotenv import load_dotenv
from database import Database
import duckdb

# Load environment variables
load_dotenv()

# Configuration for DuckDB and PostgreSQL
DUCKDB_CONFIG = {"filepath": "./data/my_duckdb.db"}

POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT'))
}


def transfer_data():
    """
    Transfer data from PostgreSQL to DuckDB.
    """
    print("Initializing DuckDB connection...")
    duck = Database(DUCKDB_CONFIG, "duckdb")

    with duck.connect() as conn:
        try:
            print("Custom extensions directory set.")
            conn.execute(
                "SET extension_directory = '/home/ipaersatz/georges/database-performance-comparison/.venv/lib/python3.11/site-packages/duckdb/extensions';"
            )
            print("Loading Postgres...")
            conn.load_extension(
                "/home/ipaersatz/georges/database-performance-comparison/.venv/lib/python3.11/site-packages/duckdb/extensions/v1.1.3/linux_amd64_gcc4/postgres_scanner.duckdb_extension"
            )

            print("Attaching PostgreSQL database to DuckDB...")
            conn.execute(
                f"""
                ATTACH 'dbname={POSTGRES_CONFIG['dbname']}
                    user={POSTGRES_CONFIG['user']}
                    host={POSTGRES_CONFIG['host']}
                    port={POSTGRES_CONFIG['port']}
                    password={POSTGRES_CONFIG['password']}'
                AS pg (TYPE POSTGRES, READ_ONLY);
            """
            )

            print(f"Attaching target DuckDB database: {DUCKDB_TARGET_CONFIG['filepath']}")
            conn.execute(f"ATTACH '{DUCKDB_TARGET_CONFIG['filepath']}' AS target_db;")

            print("Copying data from PostgreSQL to DuckDB...")
            conn.execute("COPY FROM DATABASE pg INTO target_db;")

            print("Database copied successfully.")

        except Exception as e:
            print(f"Error during data transfer: {e}")
            raise


if __name__ == "__main__":
    transfer_data()
