import threading
import psutil
import csv
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum

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
    global monitoring


    try:
        # Start monitoring
        monitor_thread = threading.Thread(target=monitor_resources, args=("resource_usage_cassandra_1.csv", 1))
        monitor_thread.start()

        start_time = time.time()

        # Execute Spark job (the code above)
        spark = SparkSession.builder \
            .appName("CassandraPunctualityAnalysis") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.cassandra.connection.port", "9042") \
            .getOrCreate()

        # Load data from Cassandra
        puekt_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="puenkt", keyspace="qdaba") \
            .load()

        # Perform aggregation
        result_df = puekt_df.groupBy("betriebstag") \
            .agg(
            _sum(when(col("tu_delta") <= 0, 1).otherwise(0)).alias("punctual_trips"),
            _sum(when(col("tu_delta") > 0, 1).otherwise(0)).alias("delayed_trips")
        )
        end_time = time.time()
        monitoring = False  # Stop monitoring

    except Exception as e:
        monitoring = False  # Stop resource monitoring in case of error
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    execute_query()