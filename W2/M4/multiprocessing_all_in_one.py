import multiprocessing
import time
from queue import Empty

def worker(tasks_to_accomplish, tasks_that_are_done):
    while True:
        try:
            task = tasks_to_accomplish.get_nowait()
        except Empty:
            break
        else:
            print(f"Task no {task}")
            time.sleep(0.5)
            result = f"Task no {task} is done by {multiprocessing.current_process().name}"
            tasks_that_are_done.put(result)
    pass

def main():
    tasks_to_accomplish = multiprocessing.Queue()
    tasks_that_are_done = multiprocessing.Queue()

    for i in range(10):
        tasks_to_accomplish.put(i)

    processes = []
    for _ in range(4):
        p = multiprocessing.Process(target=worker, args=(tasks_to_accomplish, tasks_that_are_done))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    while not tasks_that_are_done.empty():
        print(tasks_that_are_done.get())

if __name__ == '__main__':
    main()