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
monitoring = True

def monitor_resources(interval=0.1):
    """Monitor CPU and memory usage."""
    global monitoring, cpu_usage, memory_usage, timestamps
    
    while monitoring:
        cpu_usage.append(psutil.cpu_percent(interval=None))
        memory_usage.append(psutil.virtual_memory().total >> 20)  # Convert to MB
        timestamps.append(time.time())
        time.sleep(interval)

def execute_query():
    """Executes the SQL query and measures execution time."""
    global monitoring
    try:
        # Initialize database connection
        connection = psycopg2.connect(**POSTGRES_CONFIG)
        query_path = './query/postgres_punctuality.sql'

        with connection.cursor() as cursor:
            try:
                # Read the SQL query from the file
                with open(query_path, 'r') as query_file:
                    query = query_file.read()

                # Start the monitoring thread
                monitor_thread = threading.Thread(target=monitor_resources, daemon=True)
                monitor_thread.start()

                # Execute the query and measure time
                print("Executing query...")
                start_time = time.time()
                cursor.execute(query)
                connection.commit()
                end_time = time.time()
                monitoring = False  # Stop monitoring

                print("Query executed successfully.")
                print(f"Execution time: {end_time - start_time:.3f} seconds")

                # Wait for the monitor thread to finish
                monitor_thread.join()

            except Exception as query_error:
                print(f"An error occurred during query execution: {query_error}")

    except Exception as connection_error:
        print(f"Database connection failed: {connection_error}")

    finally:
        # Ensure the connection is closed properly
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")

def plot_results():
    # Ensure the directory exists
    os.makedirs('./plots', exist_ok=True)

    """Plots CPU and memory usage over time."""
    plt.figure(figsize=(12, 6))

    # Convert timestamps to elapsed time
    elapsed_time = [t - timestamps[0] for t in timestamps]

    # Plot CPU usage
    plt.subplot(2, 1, 1)
    plt.plot(elapsed_time, cpu_usage, label="CPU Usage (%)")
    plt.ylabel("CPU Usage (%)")
    plt.legend()
    plt.grid()

    # Plot memory usage
    plt.subplot(2, 1, 2)
    plt.plot(elapsed_time, memory_usage, label="Memory Usage (MB)", color="orange")
    plt.xlabel("Elapsed Time (s)")
    plt.ylabel("Memory Usage (MB)")
    plt.legend()
    plt.grid()

    # Save the plot
    output_path = './plots/resource_usage_1.png'
    plt.savefig(output_path)
    print(f"Plot saved to {output_path}")

    plt.tight_layout()

    # Save the plot
    output_path = './plots/resource_usage2.png'
    plt.savefig(output_path)
    print(f"Plot saved to {output_path}")

    plt.show()

if __name__ == "__main__":
    execute_query()
    plot_results()
