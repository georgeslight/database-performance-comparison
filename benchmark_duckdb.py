import os
import time
import psutil
import threading
import csv
from dotenv import load_dotenv
import duckdb
import matplotlib.pyplot as plt

load_dotenv()

# Global variables to control monitoring
monitoring = True

def monitor_resources(file_path, interval=1):
    """Monitor CPU, memory, and disk usage and write directly to a CSV file."""
    global monitoring
    
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "CPU Usage (%)", "Memory Usage (%)", "Disk Usage (%)"])
        
        while monitoring:
            start = time.time()
            # Get memory status
            memory = psutil.virtual_memory().percent
            # Get CPU usage
            cpu = psutil.cpu_percent(interval=None)
            # Get Disk usage
            disk = psutil.disk_usage('/').percent
            # Execution time
            timestamp = time.time()

            # Write metrics
            writer.writerow([timestamp, cpu, memory, disk])
            file.flush()  # Ensure data is written to file

            # Ensure consistent intervals
            elapsed = time.time() - start
            if elapsed < interval:
                time.sleep(interval - elapsed)

def execute_query():
    """Executes the SQL query and measures execution time."""
    global monitoring

    try:
        query_path = './query/duckdb_punctuality.sql'

        with duckdb.connect("my_duckdb.db") as con:
            # Read the SQL query from the file
            with open(query_path, 'r') as query_file:
                query = query_file.read()

            # Start the monitoring thread
            monitor_thread = threading.Thread(target=monitor_resources, args=("resource_usage_duck.csv", 1))
            monitor_thread.start()

            # Execute the query and measure time
            print("Executing query...")
            start_time = time.time()
            con.execute(query)
            end_time = time.time()
            monitoring = False  # Stop monitoring

            # Wait for the monitor thread to finish
            monitor_thread.join()

            print("Query executed successfully.")
            print(f"Execution time: {end_time - start_time:.3f} seconds")

    except Exception as e:
        monitoring = False  # Stop resource monitoring in case of error
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    execute_query()