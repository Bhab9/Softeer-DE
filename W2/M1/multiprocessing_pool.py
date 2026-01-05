import time
import multiprocessing

def work_log(task):
    """
    Define a function (work_log) that simulates each task by printing a waiting message and then sleeping for the specified duration.
    """
    name, duration = task
    print(f"Process {name} waiting {duration} seconds")
    time.sleep(duration)
    print(f"Process {name} Finished")

if __name__ == "__main__":
    work = [
        ("A", 5),
        ("B", 2),
        ("C", 1),
        ("D", 3),
    ]

    with multiprocessing.Pool(2) as pool:
        pool.map(work_log, work)