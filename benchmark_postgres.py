import os
import time
import psutil
from dotenv import load_dotenv
from database import Database

load_dotenv()


POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT'))
}


def execute_query():
    """Executes the SQL query and measures execution time."""

    postgres = Database(POSTGRES_CONFIG, 'postgres')
    view_path = './query/v_dpm_vorberechnet_neu.sql'

    with postgres.connect().cursor() as conn:
        try:
            query = open(view_path).read()
            # Measure resource usage before execution
            process = psutil.Process()
            start_cpu = process.cpu_percent(interval=None)
            start_memory = process.memory_info().rss

            # Execute the query and measure time
            start_time = time.time()
            conn.execute(query)
            conn.commit()
            end_time = time.time()

            # Measure resource usage after execution
            end_cpu = process.cpu_percent(interval=None)
            end_memory = process.memory_info().rss

            # Output results
            print(f"Query executed successfully.")
            print(f"Execution time: {end_time - start_time:.2f} seconds")
            print(f"CPU usage: {end_cpu - start_cpu:.2f}%")
            print(f"Memory usage: {(end_memory - start_memory) / (1024 ** 2):.2f} MB")


        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    execute_query()