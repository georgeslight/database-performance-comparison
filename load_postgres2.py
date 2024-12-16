import os
from dotenv import load_dotenv
from multiprocessing import Process
from database import Database

load_dotenv()

# Source and target database configurations
QDABABAV_POSTGRES_CONFIG = {
    'dbname': os.getenv('QDABABAV_POSTGRES_DBNAME'),
    'user': os.getenv('QDABABAV_POSTGRES_USER'),
    'password': os.getenv('QDABABAV_POSTGRES_PASSWORD'),
    'host': os.getenv('QDABABAV_POSTGRES_HOST'),
    'port': int(os.getenv('QDABABAV_POSTGRES_PORT'))
}

POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT'))
}

DDL_FILE = './data/puenkt_ddl.sql'

def create_table_if_not_exists(ddl_file):
    """
    Ensures the target table exists by executing the DDL statement.
    """
    try:
        postgres = Database(POSTGRES_CONFIG, 'postgres')
        postgres.connect()
        cursor = postgres.connection.cursor()

        # Read the DDL from the file
        with open(ddl_file, 'r') as ddl:
            ddl_statement = ddl.read()

        # Execute the DDL
        print("Creating table if not exists...")
        cursor.execute(ddl_statement)
        postgres.connection.commit()
        print("Table created or already exists.")

    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()
        postgres.connection.close()

def get_id_range(source_table):
    """
    Query the source database to get the minimum and maximum id_t_puenkt values.
    """
    try:
        qdaba = Database(QDABABAV_POSTGRES_CONFIG, 'postgres')
        qdaba.connect()
        qdaba_cursor = qdaba.connection.cursor()

        query = f"SELECT MIN(id_t_puenkt), MAX(id_t_puenkt) FROM {source_table};"
        qdaba_cursor.execute(query)
        min_id, max_id = qdaba_cursor.fetchone()
        print(f"ID range in source table ({source_table}): {min_id} to {max_id}")

        qdaba_cursor.close()
        qdaba.connection.close()

        return min_id, max_id
    except Exception as e:
        print(f"Error fetching ID range: {e}")
        raise

def migrate_chunk(start, end, chunk_id, source_table, target_table):
    """
    Migrate a chunk using PostgreSQL's COPY command for speed.
    """
    try:
        # Read from the source using COPY TO STDOUT
        read_command = f"""
        psql "host={QDABABAV_POSTGRES_CONFIG['host']} port={QDABABAV_POSTGRES_CONFIG['port']} \
              dbname={QDABABAV_POSTGRES_CONFIG['dbname']} user={QDABABAV_POSTGRES_CONFIG['user']} \
              password={QDABABAV_POSTGRES_CONFIG['password']}" \
        -c "\\COPY (SELECT * FROM {source_table} WHERE id_t_puenkt >= {start} AND id_t_puenkt < {end}) TO STDOUT"
        """

        # Write to the target using COPY FROM STDIN
        write_command = f"""
        psql "host={POSTGRES_CONFIG['host']} port={POSTGRES_CONFIG['port']} \
              dbname={POSTGRES_CONFIG['dbname']} user={POSTGRES_CONFIG['user']} \
              password={POSTGRES_CONFIG['password']}" \
        -c "\\COPY {target_table} FROM STDIN"
        """

        # Execute read and write commands in a single pipeline
        os.system(f"{read_command} | {write_command}")
        print(f"Chunk {chunk_id} (Rows {start}-{end}) migrated successfully.")

    except Exception as e:
        print(f"Error in chunk {chunk_id}: {e}")

def calculate_chunks(min_id, max_id, total_processes):
    """
    Calculate chunk ranges based on min and max IDs and processes.
    """
    chunk_size = (max_id - min_id + 1) // total_processes
    chunks = []
    for i in range(total_processes):
        start = min_id + i * chunk_size
        end = min_id + (i + 1) * chunk_size if i != total_processes - 1 else max_id + 1
        chunks.append((start, end))
    return chunks

def main():
    source_table = 'qdababav.puenkt'
    target_table = 'qdaba.puenkt'

    # Create table if not exists
    create_table_if_not_exists(DDL_FILE)

    # Dynamically fetch the ID range
    min_id, max_id = get_id_range(source_table)

    # Number of processes to use
    total_processes = 4

    # Calculate chunks
    chunks = calculate_chunks(min_id, max_id, total_processes)

    # Parallel processing
    processes = []
    for chunk_id, (start, end) in enumerate(chunks):
        print(f"Starting process: {chunk_id} for range {start} to {end}")
        process = Process(target=migrate_chunk, args=(start, end, chunk_id, source_table, target_table))
        processes.append(process)
        process.start()

    # Wait for all processes to complete
    for process in processes:
        process.join()

    print("All chunks migrated successfully.")

if __name__ == "__main__":
    main()
