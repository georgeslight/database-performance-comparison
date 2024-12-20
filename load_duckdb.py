import os
import logging
from dotenv import load_dotenv
from database import Database
import duckdb

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration for DuckDB and PostgreSQL
DUCKDB_CONFIG = {"filepath": "./data/my_duckdb.db"}

QDABABAV_POSTGRES_CONFIG = {
    'dbname': os.getenv('QDABABAV_POSTGRES_DBNAME'),
    'user': os.getenv('QDABABAV_POSTGRES_USER'),
    'password': os.getenv('QDABABAV_POSTGRES_PASSWORD'),
    'host': os.getenv('QDABABAV_POSTGRES_HOST'),
    'port': int(os.getenv('QDABABAV_POSTGRES_PORT'))
}

def transfer_data():
    """
    Transfer data from PostgreSQL to DuckDB.
    """
    logging.info("Initializing DuckDB connection...")
    duck = Database(DUCKDB_CONFIG, 'duckdb')
    # print("SET unsigned extensions true...")
    # conn.execute("SET allow_unsigned_extensions = true;")

    with duck.connect() as conn:
        try:
            print("Custom extensions directory set.")
            conn.execute("SET extension_directory = '/home/ipaersatz/georges/database-performance-comparison/.venv/lib/python3.11/site-packages/duckdb/extensions';")
            # conn.install_extension("/home/ipaersatz/georges/database-performance-comparison/.venv/lib/python3.11/site-packages/duckdb/extension/postgres_scanner/postgres_scanner.duckdb_extension")
            print("Loading Postgres...")
            conn.load_extension("/home/ipaersatz/georges/database-performance-comparison/.venv/lib/python3.11/site-packages/duckdb/extensions/v1.1.3/linux_amd64_gcc4/postgres_scanner.duckdb_extension")

            logging.info("Attaching PostgreSQL database to DuckDB...")
            conn.execute(f"""
                ATTACH 'dbname={QDABABAV_POSTGRES_CONFIG['dbname']}
                    user={QDABABAV_POSTGRES_CONFIG['user']}
                    host={QDABABAV_POSTGRES_CONFIG['host']}
                    port={QDABABAV_POSTGRES_CONFIG['port']}
                    password={QDABABAV_POSTGRES_CONFIG['password']}'
                AS pg (TYPE POSTGRES, READ_ONLY);
            """)

            logging.info("Creating schema in DuckDB...")
            conn.execute("CREATE SCHEMA IF NOT EXISTS qdaba;")

            logging.info("Copying data from PostgreSQL to DuckDB...")
            conn.execute("CREATE TABLE qdaba.puenkt AS FROM pg.qdababav.puenkt;")
            logging.info("Data copied successfully.")

            # Verify row counts
            duckdb_count = conn.execute("SELECT COUNT(*) FROM qdaba.puenkt;").fetchone()[0]
            postgres_count = conn.execute("SELECT COUNT(*) FROM pg.qdababav.puenkt;").fetchone()[0]
            logging.info(f"Rows in DuckDB table: {duckdb_count}")
            logging.info(f"Rows in PostgreSQL table: {postgres_count}")

        except Exception as e:
            logging.error(f"Error during data transfer: {e}")
            raise

if __name__ == '__main__':
    transfer_data()
