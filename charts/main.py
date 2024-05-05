import csv
import matplotlib.pyplot as plt

# Lists to store iteration numbers and JDBC execution times
iterations = []
jdbc_times = []

# Read data from CSV file
with open('MariaDB_results/update_times.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header
    for row in reader:
        iterations.append(int(row[0]))
        jdbc_times.append(float(row[1]))

# Plot the data
plt.plot(iterations, jdbc_times, marker='o', linestyle='-')
plt.xlabel('Iteration')
plt.ylabel('JDBC Execution Time (seconds)')
plt.title('JDBC Execution Time vs Iteration (1000)')
plt.grid(True)
plt.show()

iterations = []
jdbc_times = []
#
# with open('jdbc_execution_time_10000.csv', 'r') as file:
#     reader = csv.reader(file)
#     next(reader)  # Skip header
#     for row in reader:
#         iterations.append(int(row[0]))
#         jdbc_times.append(float(row[1]))
#
# # Plot the data
# plt.plot(iterations, jdbc_times, marker='o', linestyle='-')
# plt.xlabel('Iteration')
# plt.ylabel('JDBC Execution Time (seconds)')
# plt.title('JDBC Execution Time vs Iteration (10000)')
# plt.grid(True)
# plt.show()
