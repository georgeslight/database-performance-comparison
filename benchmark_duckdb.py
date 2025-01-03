import os
import time
import psutil
from dotenv import load_dotenv
from database import Database

load_dotenv()

DUCKDB_CONFIG = {"filepath": "./data/my_duckdb.db"}


def execute_query():
    """Executes the SQL query and measures execution time."""

    duck = Database(DUCKDB_CONFIG, 'duckdb')
    view_path = './query/v_dpm_vorberechnet_neu.sql'

    with duck.connect() as conn:
        try:
            cur = conn.cursor()
            # Read the query file safely
            with open(view_path, 'r') as query_file:
                query = query_file.read()

            # Measure resource usage before execution
            process = psutil.Process()
            start_cpu = process.cpu_percent(interval=0.1)  # Short interval to get initial CPU usage
            start_memory = process.memory_info().rss

            # Execute the query and measure time
            start_time = time.time()
            cur.execute(query)
            conn.commit()  # Commit changes at the connection level
            end_time = time.time()

            # Measure resource usage after execution
            end_cpu = process.cpu_percent(interval=0.1)  # Short interval to capture final CPU usage
            end_memory = process.memory_info().rss

            # Output results
            print(f"Query executed successfully.")
            print(f"Execution time: {end_time - start_time:.2f} seconds")
            print(f"CPU usage: {end_cpu - start_cpu:.2f}%")
            print(f"Memory usage: {(end_memory - start_memory) / (1024 ** 2):.2f} MB")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            # Always ensure cursor and connection are closed
            cur.close()


if __name__ == "__main__":
    execute_query()
