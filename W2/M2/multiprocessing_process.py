import multiprocessing

def print_continent(continent="Asia"):
    print(f"The name of continent is : {continent}")

if __name__ == "__main__":
    processes = []

    # once with default continent
    p_def = multiprocessing.Process(target=print_continent)
    processes.append(p_def)

    # three times with different continent names
    continents = ["America", "Europe", "Africa"]
    for name in continents:
        p = multiprocessing.Process(target=print_continent, args=(name, ))
        processes.append(p)

    for p in processes:
        p.start()

    for p in processes:
        p.join()