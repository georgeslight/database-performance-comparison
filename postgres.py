import io
import os
from dotenv import load_dotenv
from database import Database

load_dotenv()

# Source DB configuration
QDABABAV_POSTGRES_CONFIG = {
    'dbname': os.getenv('QDABABAV_POSTGRES_DBNAME'),
    'user': os.getenv('QDABABAV_POSTGRES_USER'),
    'password': os.getenv('QDABABAV_POSTGRES_PASSWORD'),
    'host': os.getenv('QDABABAV_POSTGRES_HOST'),
    'port': int(os.getenv('QDABABAV_POSTGRES_PORT'))
}

# Target DB configuration
POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT'))
}

try:
    source_table = 'qdababav.puenkt'
    target_table = 'qdaba.puenkt'

    # Connect to source and target databases
    source_db = Database(QDABABAV_POSTGRES_CONFIG, 'postgres')
    source_db.connect()
    target_db = Database(POSTGRES_CONFIG, 'postgres')
    target_db.connect()

    # Process data month by month
    for month in range(1, 13):
        print(f"Processing data for month {month} of 2024...")

        # Create an in-memory stream for the data
        data_stream = io.StringIO()

        # Export data from the source database into the stream
        with source_db.connection.cursor() as source_cursor:
            query = f"""
                COPY (
                    SELECT * FROM {source_table}
                    WHERE EXTRACT(YEAR FROM betriebstag) = 2024
                      AND EXTRACT(MONTH FROM betriebstag) = {month}
                ) TO STDOUT WITH CSV HEADER;
            """
            source_cursor.copy_expert(query, data_stream)

        # Reset the stream position to the beginning
        data_stream.seek(0)

        # Import data from the stream into the target database
        with target_db.connection.cursor() as target_cursor:
            target_cursor.copy_expert(f"""
                COPY {target_table} FROM STDIN WITH CSV HEADER;
            """, data_stream)

        # Commit the transaction
        target_db.connection.commit()

        print(f"Month {month}: Data transferred successfully.")

    print("All months processed successfully.")

except Exception as e:
    print(f"Error: {e}")
finally:
    # Close database connections
    source_db.connection.close()
    target_db.connection.close()
