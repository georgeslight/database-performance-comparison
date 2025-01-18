import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load the CSV data
file_path = './csv/resource_usage_duck_final_2.csv'
data = pd.read_csv(file_path, skipinitialspace=True)

# Strip column names of any extra spaces
data.columns = data.columns.str.strip()

# Check if the 'Timestamp' column exists
if 'Elapsed Seconds' not in data.columns:
    print("Error: 'Elapsed Seconds' column not found in the CSV file. Check your file and try again.")
    print(f"Available columns: {data.columns.tolist()}")
    exit(1)

# Convert timestamps to relative time (seconds)
start_time = data['Elapsed Seconds'].min()
data['Time (s)'] = data['Elapsed Seconds'] - start_time

# Calculate averages
avg_cpu = data['CPU Usage (%)'].mean()
avg_memory = data['Memory Usage (%)'].mean()
avg_disk = data['Disk Usage (%)'].mean()

# Plot settings
def plot_resource_usage(data, column, avg, color, ax):
    ax.plot(data['Time (s)'], data[column], color=color, alpha=0.8, label=f'{column} Usage')
    ax.fill_between(data['Time (s)'], data[column], alpha=0.2, color=color)
    ax.axhline(avg, linestyle='--', color=color, label=f'Average {column}: {avg:.2f}%')
    ax.set_title(f'{column} Over Time')
    ax.set_xlabel('Time (s)')
    ax.set_ylabel(f'{column}')
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.6)

# Create subplots for each resource
fig, axs = plt.subplots(3, 1, figsize=(12, 18), sharex=True)

plot_resource_usage(data, 'CPU Usage (%)', avg_cpu, 'blue', axs[0])
plot_resource_usage(data, 'Memory Usage (%)', avg_memory, 'green', axs[1])
plot_resource_usage(data, 'Disk Usage (%)', avg_disk, 'red', axs[2])

# Final adjustments
fig.tight_layout()
plt.show()

# Print summary statistics
print(f"Time Analyzed: {data['Time (s)'].iloc[-1]:.2f} seconds")
print(f"Average CPU Usage: {avg_cpu:.2f}%")
print(f"Average Memory Usage: {avg_memory:.2f}%")
print(f"Average Disk Usage: {avg_disk:.2f}%")