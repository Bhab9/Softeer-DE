import multiprocessing

def main():
    colors = ["red", "green", "blue", "black"]
    queue = multiprocessing.Queue()

    print("pushing items to queue:")
    for idx, color in enumerate(colors, start=1):
        queue.put(color)
        print(f"item no: {idx} {color}")

    print("popping items from queue:")
    count = 0
    while not queue.empty():
        item = queue.get()
        print(f"item no: {count} {item}")
        count += 1

if __name__ == "__main__":
    main()