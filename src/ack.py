import json
import zmq
from tabulate import tabulate

def listen_for_acks():
    context = zmq.Context()
    
    # Create a SUB socket to subscribe to the publisher
    subscriber = context.socket(zmq.SUB)
    
    # Bind the SUB socket to the specified address
    subscriber.bind("tcp://127.0.0.1:5558")
    
    # Set the subscription option to receive all messages (an empty string subscribes to all topics)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, "")
    
    try:
        while True:
            # Receive acknowledgement messages
            ack_msg = subscriber.recv_string()
            
            # Check if the message is an order book message
            if ack_msg.startswith('order_book;'):
                # Parse the JSON data from the message
                order_book_data = json.loads(ack_msg.split(';', 1)[1])
                
                # Format the bids and asks data for tabulate
                bids_data = order_book_data.get('bids', [])
                asks_data = order_book_data.get('asks', [])
                
                # Tabulate the bids and asks data
                bids_table = tabulate(bids_data, headers='keys', tablefmt='pretty')
                asks_table = tabulate(asks_data, headers='keys', tablefmt='pretty')
                
                print(f"Bids:\n{bids_table}\nAsks:\n{asks_table}")
            else:
                print(f"Received Ack: {ack_msg}")
    except KeyboardInterrupt:
        print("Stopped listening for acks.")
    finally:
        # Close the ZMQ subscriber socket
        subscriber.close()

# Call the function to start listening for acks
listen_for_acks()

# poetry run python ack.py