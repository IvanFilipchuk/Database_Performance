import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

def read_csv_files(directory):
    data = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
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
    databases = ['couchbase','mariadb','postgresql', 'redis']
    colors = ['r', 'g', 'b', 'y']
    for op in operations:
        plt.figure(figsize=(10, 6))
        plotted_labels = set()
        for db, color in zip(databases, colors):
            key = f"{db}_{op}"
            if key in data:
                df_list = data[key]
                for df in df_list:
                    label = db if db not in plotted_labels else ""
                    plt.plot(df['index'][:num_records], df['time'][:num_records], color=color, label=label)
                    plotted_labels.add(db)
        plt.title(f'{op.capitalize()} Operation Performance')
        plt.xlabel('Records')
        plt.ylabel('Time (s)')
        plt.legend(loc='best')
        plt.grid(True)
        ax = plt.gca()
        ax.yaxis.set_major_locator(MaxNLocator(integer=True, prune='both'))
        ax.xaxis.set_major_locator(MaxNLocator(integer=True, prune='both'))
        plt.show()

def main(num_records):
    directories = ['create', 'read', 'update', 'delete']
    all_data = {}

    for directory in directories:
        data = read_csv_files(directory)
        all_data.update(data)

    plot_data(all_data, num_records)
if __name__ == "__main__":
    num_records = 100
    main(num_records)
