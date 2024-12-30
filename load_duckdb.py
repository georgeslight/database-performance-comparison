import os
from dotenv import load_dotenv
from database import Database
import duckdb

# Load environment variables
load_dotenv()

# Configuration for DuckDB and PostgreSQL
DUCKDB_CONFIG = {"filepath": "./data/my_duckdb.db"}

QDABABAV_POSTGRES_CONFIG = {
    "dbname": os.getenv("QDABABAV_POSTGRES_DBNAME"),
    "user": os.getenv("QDABABAV_POSTGRES_USER"),
    "password": os.getenv("QDABABAV_POSTGRES_PASSWORD"),
    "host": os.getenv("QDABABAV_POSTGRES_HOST"),
    "port": int(os.getenv("QDABABAV_POSTGRES_PORT")),
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
                ATTACH 'dbname={QDABABAV_POSTGRES_CONFIG['dbname']}
                    user={QDABABAV_POSTGRES_CONFIG['user']}
                    host={QDABABAV_POSTGRES_CONFIG['host']}
                    port={QDABABAV_POSTGRES_CONFIG['port']}
                    password={QDABABAV_POSTGRES_CONFIG['password']}'
                AS pg (TYPE POSTGRES, READ_ONLY);
            """
            )

            print("Creating schema in DuckDB...")
            conn.execute("CREATE SCHEMA IF NOT EXISTS qdaba;")

            tables_to_copy = [
                "puenkt",
                "import",
                "anwendungsfall",
                "bhf",
                "mvu",
                "linie",
            ]

            for table in tables_to_copy:
                print(f"Copying {table} table from PostgreSQL to DuckDB...")
                conn.execute(f"CREATE TABLE qdaba.{table} AS FROM pg.qdababav.{table};")
                print(f"{table} table copied successfully.")

            print("Data copied successfully.")

        except Exception as e:
            print(f"Error during data transfer: {e}")
            raise


if __name__ == "__main__":
    transfer_data()
