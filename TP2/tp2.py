import sys, time, threading
import psutil
import pandas as pd
import dask
import dask.dataframe as dd
import matplotlib.pyplot as plt

CSV = r"C:\Users\Aymen\Desktop\BigData\TP2\train.csv"
CHUNK = 100_000
COLS = ["fare_amount"]


def measure(func):
    peak = 0
    running = True

    def watch():
        nonlocal peak
        while running:
            ram = psutil.Process().memory_info().rss / 1024**2
            peak = max(peak, ram)
            time.sleep(0.1)

    watcher = threading.Thread(target=watch)
    watcher.start()

    start = time.time()
    func()
    seconds = time.time() - start

    running = False
    watcher.join()
    return seconds, peak

def task1():
    try:
        pd.read_csv(CSV)
    except MemoryError as e:
        print("MemoryError:", e)

def task2():
    total = 0.0
    count = 0

    for chunk in pd.read_csv(CSV, chunksize=CHUNK, usecols=COLS):
        total += chunk["fare_amount"].sum()
        count += len(chunk)

    print(f"Rows: {count:,}")
    print(f"Sum:{total}")
    print(f"Mean fare: {total / count:.2f}")


# Task 3: Dask (lazy)
def task3():
    ddf = dd.read_csv(CSV, blocksize="64MB", usecols=COLS)
    print("Partitions:", ddf.npartitions)

    mean_fare = ddf["fare_amount"].mean()

    print("Graph size:", len(mean_fare.__dask_graph__()))
    mean_fare = mean_fare.optimize()
    print("Optimized graph size:", len(mean_fare.__dask_graph__()))

    result = mean_fare.compute()
    print(f"Mean fare: {result:.2f}")


if __name__ == "__main__":
    results = {}
    results["Pandas chunking"] = measure(task2)
    results["Dask"] = measure(task3)
    results["Standard pandas"] = measure(task1)      # last, it is the heavy one

    # print the table in the TP order
    order = ["Standard pandas", "Pandas chunking", "Dask"]
    print("\nMethod              Time (s)   Peak RAM (MB)")
    for name in order:
        s, mb = results[name]
        print(f"{name:<19} {s:8.1f}   {mb:10.0f}")

    times = [results[n][0] for n in order]
    rams = [results[n][1] for n in order]

    fig, ax = plt.subplots(1, 2, figsize=(11, 4))

    bars = ax[0].bar(order, times, color=["tab:red", "tab:orange", "tab:green"])
    ax[0].set_title("Execution time")
    ax[0].set_ylabel("Seconds")
    ax[0].bar_label(bars, fmt="%.1f")

    bars = ax[1].bar(order, rams, color=["tab:red", "tab:orange", "tab:green"])
    ax[1].set_title("Peak RAM usage")
    ax[1].set_ylabel("MB")
    ax[1].bar_label(bars, fmt="%.0f")

    plt.tight_layout()
    plt.savefig("benchmark.png", dpi=150)
    plt.show()