import os
import time
import psutil
import threading
import csv
from dotenv import load_dotenv
import duckdb
import matplotlib.pyplot as plt

load_dotenv()

def get_next_filename(extension=".csv"):
    """Finds the next available filename by scanning all CSV files and using the highest number + 1."""
    
    existing_files = [f for f in os.listdir() if f.endswith(extension)]
    existing_numbers = []

    for filename in existing_files:
        try:
            # Split filename by underscores and take the last part before .csv
            number_part = filename.split('_')[-1].replace(extension, "")
            number = int(number_part)  # Convert to integer
            existing_numbers.append(number)
        except ValueError:
            continue  # Ignore files that don't have a number

    next_number = max(existing_numbers) + 1 if existing_numbers else 1
    return f"duckdb_by_hour_{next_number}{extension}"

# Global variables to control monitoring
monitoring = True

def monitor_resources(file_path, interval=1):
    """Monitor CPU, memory, and disk usage and write directly to a CSV file."""
    global monitoring
    
    start_time = time.time()  # Record the start time
    
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Elapsed Seconds", "CPU Usage (%)", "Memory Usage (%)", "Disk Usage (%)"])
        
        while monitoring:
            iteration_start = time.time()

            # Calculate elapsed time in seconds
            elapsed_seconds = iteration_start - start_time

            # Get memory status
            memory = psutil.virtual_memory().percent
            # Get CPU usage
            cpu = psutil.cpu_percent(interval=None)
            # Get Disk usage
            disk = psutil.disk_usage('/').percent

            # Write metrics
            writer.writerow([round(elapsed_seconds, 2), cpu, memory, disk])
            file.flush()  # Ensure data is written to file

            # Ensure consistent intervals
            elapsed = time.time() - iteration_start
            if elapsed < interval:
                time.sleep(interval - elapsed)

def execute_query():
    """Executes the SQL query and measures execution time."""
    global monitoring

    try:
        query_path = './query/duckdb_by_hour.sql'

        with duckdb.connect("my_new_duckdb.db") as con:
            # Read the SQL query from the file
            with open(query_path, 'r') as query_file:
                query = query_file.read()

            # Get the next available filename
            temp_file_path = get_next_filename()

            # Start the monitoring thread
            monitor_thread = threading.Thread(target=monitor_resources, args=(temp_file_path, 1))
            monitor_thread.start()

            # Execute the query and measure time
            print("Executing query...")
            start_time = time.time()
            con.execute(query)
            end_time = time.time()
            monitoring = False  # Stop monitoring

            # Wait for the monitor thread to finish
            monitor_thread.join()

            execution_time = round(end_time - start_time, 2)
            new_file_path = f"{execution_time}_{temp_file_path}"
            # Rename the file
            os.rename(temp_file_path, new_file_path)

            print("Query executed successfully.")
            print(f"Execution time: {end_time - start_time:.2f} seconds")

    except Exception as e:
        monitoring = False  # Stop resource monitoring in case of error
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    execute_query()