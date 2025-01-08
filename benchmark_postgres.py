import os
import time
import psutil
from dotenv import load_dotenv
from database import Database
import psycopg2

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
    try:
        # Initialize database connection
        connection = psycopg2.connect(**POSTGRES_CONFIG)

        query_path = './query/punctuality.sql'

        with connection.cursor() as cursor:
            try:
                # Read the SQL query from the file
                with open(query_path, 'r') as query_file:
                    query = query_file.read()

                # Measure resource usage before execution
                process = psutil.Process()
                start_cpu = process.cpu_percent(interval=None)
                start_memory = process.memory_info().rss

                # Execute the query and measure time
                start_time = time.time()
                cursor.execute(query)
                connection.commit()
                end_time = time.time()

                # Measure resource usage after execution
                end_cpu = process.cpu_percent(interval=None)
                end_memory = process.memory_info().rss

                # Output results
                print("Query executed successfully.")
                print(f"Execution time: {end_time - start_time:.3f} seconds")
                print(f"CPU usage: {end_cpu - start_cpu:.3f}%")
                print(f"Memory usage: {(end_memory - start_memory) / (1024 ** 2):.3f} MB")

            except Exception as query_error:
                print(f"An error occurred during query execution: {query_error}")

    except Exception as connection_error:
        print(f"Database connection failed: {connection_error}")

    finally:
        # Ensure the connection is closed properly
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    execute_query()
