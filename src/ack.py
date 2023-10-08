import json
import zmq
from tabulate import tabulate

def pretty_ack(ack_msg):
    field_names = {
        '35': 'Message Type',
        '49': 'Sender Comp ID',
        '37': 'Order ID',
        '38': 'Order Quantity',
        '40': 'Order Type',
        '44': 'Price',
        '52': 'Sending Time',
        '54': 'Side',
        '6404': 'POV Target Percentage',
        '55': 'Trading Pair'
    }

    def parse_ack_segment(segment):
        # Split the acknowledgment segment by semicolon
        segments = segment.split(';')
        # Create a dictionary to hold the parsed segments
        ack_data = {}
        for seg in segments:
            key, sep, value = seg.partition('=')
            # Use the human-readable field name if available, otherwise use the field tag
            field_name = field_names.get(key, key)
            ack_data[field_name] = value

        # Use string formatting to create a structured output
        formatted_ack = '\n'.join([f"{key}: {value}" for key, value in ack_data.items()])
        return formatted_ack

    # Check if the message contains a list of acknowledgment messages
    if ack_msg.startswith('search_order;'):
        ack_list = eval(ack_msg.split(';', 1)[1])
        parsed_acks = [parse_ack_segment(ack) for ack in ack_list]
        formatted_ack = '\n---\n'.join(parsed_acks)
    else:
        formatted_ack = parse_ack_segment(ack_msg)
    
    return formatted_ack


def filter_and_format_data(data):
    # Separate the rows where 'SenderCompID' is 'EXCHANGE' and not 'EXCHANGE'
    exchange_rows = [row for row in data if row.get('SenderCompID') == 'EXCHANGE']
    non_exchange_rows = [row for row in data if row.get('SenderCompID') != 'EXCHANGE']
    
    # Highlight non-EXCHANGE rows
    highlighted_non_exchange_rows = []
    for row in non_exchange_rows:
        highlighted_row = {k: f"**{v}**" if k == 'SenderCompID' and v != 'EXCHANGE' else v for k, v in row.items()}
        highlighted_non_exchange_rows.append(highlighted_row)
    
    # Combine the rows, ensuring at least one EXCHANGE row is included
    combined_rows = highlighted_non_exchange_rows + exchange_rows[:10] 
    # Take the first 10 rows
    limited_rows = combined_rows[:10]
    
    return limited_rows

def print_order_book(order_book_data):
    bids_data = order_book_data.get('bids', [])
    asks_data = order_book_data.get('asks', [])
    
    # Filter and format the data
    formatted_bids_data = filter_and_format_data(bids_data)
    formatted_asks_data = filter_and_format_data(asks_data)
    
    # Tabulate the filtered and formatted data
    bids_table = tabulate(formatted_bids_data, headers='keys', tablefmt='pretty')
    asks_table = tabulate(formatted_asks_data, headers='keys', tablefmt='pretty')
    
    print(f"Bids:\n{bids_table}\nAsks:\n{asks_table}")

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
            print("----------start--------------")
            # Check if the message is an order book message
            if ack_msg.startswith('order_book;'):
            
                order_book_data = json.loads(ack_msg.split(';', 1)[1])
                print_order_book(order_book_data)
            else:
                print("Received Acknowledgement message:")
                print(f"Raw Ack: {ack_msg}")
                print(f"Parsed Ack: {pretty_ack(ack_msg)}\n")
            print("----------end--------------")
    except KeyboardInterrupt:
        print("Stopped listening for acks.")
    finally:
        # Close the ZMQ subscriber socket
        subscriber.close()

# Call the function to start listening for acks
listen_for_acks()

# poetry run python ack.py