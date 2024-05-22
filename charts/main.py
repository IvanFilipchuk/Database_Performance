import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

def read_csv_files(directory,num_records):
    data = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(str(num_records)+'.csv'):
                db, operation, _, count = file.split('_')
                key = f"{db}_{operation}"
                filepath = os.path.join(root, file)
                df = pd.read_csv(filepath, header=None, names=['index', 'time'])
                if key not in data:
                    data[key] = []
                data[key].append(df)
    return data

def plot_data(data, num_records):
    operations = ['import', 'select1', 'select2', 'select3', 'update1', 'update2', 'delete']
    databases = ['couchbase', 'mariadb', 'postgresql', 'redis']
    colors = ['r', 'g', 'b', 'y']
    for op in operations:
        plt.figure(figsize=(10, 6))
        min_time, max_time = float('inf'), float('-inf')
        for db, color in zip(databases, colors):
            key = f"{db}_{op}"
            if key in data:
                df_list = data[key]
                for df in df_list:
                    sorted_df = df.sort_values(by='index')
                    sorted_df['time'] = pd.to_numeric(sorted_df['time'], errors='coerce')
                    plt.plot(sorted_df['index'][:], sorted_df['time'][:], color=color, label=db)
                    min_time = min(min_time, sorted_df['time'].min(skipna=True))
                    max_time = max(max_time, sorted_df['time'].max(skipna=True))

        plt.title(f'{op.capitalize()} Operation Performance')
        plt.xlabel('Records')
        plt.ylabel('Time (s)')
        plt.legend(loc='best')
        plt.grid(True)
        ax = plt.gca()
        ax.xaxis.set_major_locator(MaxNLocator(nbins=10))
        ax.yaxis.set_major_locator(MaxNLocator(nbins=10))
        ax.set_ylim(min_time, max_time)
        plt.show()

num_records = 10000
directories = ['create', 'read', 'update', 'delete']
all_data = {}
for directory in directories:
    data = read_csv_files(directory,num_records)
    all_data.update(data)
plot_data(all_data, num_records)

