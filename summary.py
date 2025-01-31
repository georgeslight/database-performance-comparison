import pandas as pd
import sys

def process_system_usage(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Compute execution time (last elapsed second)
    execution_time = round(df['Elapsed Seconds'].max(), 2)
    
    # Compute average CPU, memory, and disk usage
    avg_cpu_usage = round(df['CPU Usage (%)'].mean(), 2)
    avg_memory_usage = round(df['Memory Usage (%)'].mean(), 2)
    avg_disk_usage = round(df['Disk Usage (%)'].mean(), 2)
    
    # Format the results
    results = pd.DataFrame({
        'RUN': [1],
        'EXECUTION TIME (S)': [execution_time],
        'CPU USAGE (%)': [avg_cpu_usage],
        'MEMORY USAGE (%)': [avg_memory_usage],
        'DISK USAGE (%)': [avg_disk_usage]
    })
    
    # Display the results
    print(results.to_string(index=False))
    
if __name__ == "__main__":
    
    file_path = "./csv/final_tests/by_hour/469.98_postgres_by_hour_5.csv"
    process_system_usage(file_path)