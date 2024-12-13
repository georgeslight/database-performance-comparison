import os
from dotenv import load_dotenv

from database import Database

puenkt_csv = 'data/puenkt.csv'
load_dotenv()

# Check if variables are loaded correctly
print(f"Source DB: {os.getenv('QDABABAV_POSTGRES_DBNAME')}")
print(f"Target DB: {os.getenv('POSTGRES_DBNAME')}")


# source
QDABABAV_POSTGRES_CONFIG = {
    'dbname': os.getenv('QDABABAV_POSTGRES_DBNAME'),
    'user': os.getenv('QDABABAV_POSTGRES_USER'),
    'password': os.getenv('QDABABAV_POSTGRES_PASSWORD'),
    'host': os.getenv('QDABABAV_POSTGRES_HOST'),
    'port': int(os.getenv('QDABABAV_POSTGRES_PORT'))
}

# target
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
    ddl_path = './data/puenkt_ddl.sql'

    # source
    qdaba = Database(QDABABAV_POSTGRES_CONFIG, 'postgres')
    qdaba.connect()
    qdaba_cursor = qdaba.connection.cursor(name='source_cursor')
    # target
    postgres = Database(POSTGRES_CONFIG, 'postgres')
    postgres.connect()
    pg_cursor = postgres.connection.cursor()

    # DDL
    try:
        with open(ddl_path, 'r') as ddl_file:
            ddl = ddl_file.read()
            print("Executing DDL")
            pg_cursor.execute(ddl)
            postgres.connection.commit()
            print("Schema and table created successfully.")
    except Exception as e:
        print(f"Error executing DDL: {e}")

    qdaba_cursor.execute(f"SELECT * FROM {source_table};")
    rows_fetched = 0

    while True:
        rows = qdaba_cursor.fetchmany(10000)
        if not rows:
            break # No more data

        rows_fetched += len(rows)

        insert_query = f"INSERT INTO {target_table} VALUES ({', '.join(['%s'] * len(rows[0]))})"
        pg_cursor.executemany(insert_query, rows)
        postgres.connection.commit()

        print(f"Loaded {rows_fetched} rows so far...")

    print(f"Loaded {rows_fetched} total rows.")
except Exception as e:
    print(f"Error: {e}")
finally:
    qdaba_cursor.close()
    qdaba.connection.close()
    pg_cursor.close()
    postgres.connection.close()