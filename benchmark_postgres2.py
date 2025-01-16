import os
import time
import psutil
import threading
from dotenv import load_dotenv
import psycopg2
import matplotlib.pyplot as plt

load_dotenv()

POSTGRES_CONFIG = {
    'dbname': os.getenv('POSTGRES_DBNAME'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': int(os.getenv('POSTGRES_PORT'))
}

# Global variables to track metrics
cpu_usage = []
memory_usage = []
timestamps = []
disk_usage = []
monitoring = True

def monitor_resources(interval=1):
    """Monitor CPU and memory usage."""
    global monitoring, cpu_usage, memory_usage, timestamps
    
    while monitoring:
        # Get memory status
        memory = psutil.virtual_memory().percent
        # Get CPU usage
        cpu = psutil.cpu_percent(interval=1)
        # Get Disk usage 
        disk = psutil.disk_usage('/')
        # Execution time
        timestamp = time.time()

        # Append data to the lists
        cpu_usage.append(cpu)
        memory_usage.append(memory)
        disk_usage.append(disk)
        timestamps.append(timestamp)

        # Wait for next interval
        time.sleep(interval)

def execute_query():
    """Executes the SQL query and measures execution time."""
    global monitoring

    try:
        # Initialize database connection
        connection = psycopg2.connect(**POSTGRES_CONFIG)
        query_path = './query/postgres_punctuality.sql'

        with connection.cursor() as cursor:
            # Read the SQL query from the file
            with open(query_path, 'r') as query_file:
                query = query_file.read()

            # Start the monitoring thread
            monitor_thread = threading.Thread(target=monitor_resources, args=(1,))
            monitor_thread.start()

            # Execute the query and measure time
            print("Executing query...")
            start_time = time.time()
            cursor.execute(query)
            end_time = time.time()
            monitoring = False  # Stop monitoring

            print("Query executed successfully.")
            print(f"Execution time: {end_time - start_time:.3f} seconds")

            # Wait for the monitor thread to finish
            monitor_thread.join()

            print("Query executed successfully.")
            print(f"Execution time: {end_time - start_time:.3f} seconds")

    except Exception as e:
        monitoring = False  # Stop resource monitoring in case of error
        print(f"An error occurred: {e}")
    finally:
        # Ensure the connection is closed properly
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")


if __name__ == "__main__":
    execute_query()

    # Save monitoring results to a file for further analysis
    with open("resource_usage.csv", "a") as file:  # Use 'a' mode to append
        # If the file is empty, write the header
        if file.tell() == 0:
            file.write("Timestamp,CPU Usage (%),Memory Usage (%),Disk Usage (%)\n")
        # Append the new data
        for ts, cpu, mem, disk in zip(timestamps, cpu_usage, memory_usage, disk_usage):
            file.write(f"{ts},{cpu},{mem},{disk}\n")