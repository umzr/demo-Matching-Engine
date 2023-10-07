import zmq
import csv
import time
import sys
import threading

def process_csv(file_path, publisher, instrument):
    print(f'Opening file: {file_path}')
    content = []
    with open(file_path, newline='') as file:
        csv_reader = csv.reader(file)  # Change from csv.DictReader to csv.reader
        for row in csv_reader:
            content.append(row)

    row_idx = 0
    while row_idx < len(content):
        row = content[row_idx]
        data = f"instrument={instrument};id={row[0]};price={row[1]};qty={row[2]};base_qty={row[3]};time={row[4]};is_buyer_maker={row[5]};"
        print(f'sent: {data}')
        data = f"Q {data}"
        publisher.send_string(data)  # Assuming you've changed the send to send_string as per previous instructions
        row_idx += 1
        time.sleep(1)  # Add a delay between sends, adjust as needed


def main():
    if len(sys.argv) != 3:
        print("Usage: python marketDataStreamer.py <path_to_csv1> <path_to_csv2>")
        return

    # Prepare the PUB streaming socket
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    publisher.bind("tcp://127.0.0.1:5556")
    time.sleep(0.2)  # Equivalent to usleep(200000)

    # Start threads to process each CSV file
    thread1 = threading.Thread(target=process_csv, args=(sys.argv[1], publisher, "BTCUSDT"))
    thread2 = threading.Thread(target=process_csv, args=(sys.argv[2], publisher, "ETHUSDT"))
    thread1.start()
    thread2.start()

    # Wait for both threads to complete
    thread1.join()
    thread2.join()

if __name__ == "__main__":
    main()


# poetry run python market_data_streamer.py data/BTCUSDT-trades.csv data/ETHUSDT-trades.csv