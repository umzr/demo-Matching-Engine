import random
import string
import zmq
import threading
import json
from typing import Dict


class TradingClient:
    order_id_counter = 0
    
    def __init__(self):
        self.context = zmq.Context()

        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.connect("tcp://127.0.0.1:5556")
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, "Q")

        self.trading_time_thread = threading.Thread(target=self.get_trading_time)
        self.trading_time_thread.start()
        self.trading_time = None
        
        
        self.order_publisher = self.context.socket(zmq.PUB)
        self.order_publisher.connect("tcp://127.0.0.1:5557")

        self.ack_subscriber = self.context.socket(zmq.SUB)
        self.ack_subscriber.connect("tcp://127.0.0.1:5558")
        self.ack_subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

        self.user_input_thread = threading.Thread(target=self.handle_user_input)
        self.user_input_thread.start()
        
        self.sender_comp_id = self.generate_random_id()

        self.trading_pair = ''
        
        self.listen_for_acks()

    def get_trading_time(self):
        while True:
            message = self.subscriber.recv_string()

            trading_time = self.parse_trading_time(message)  # Define this method to parse the message
            self.trading_time = trading_time  # Assuming a class variable to store the trading time

            
    def parse_trading_time(self, message):
        # Assuming the trading time is sent in a message formatted as "TradingTime=HH:MM:SS"
        try:
            trading_time = message.split('=')[7]
            return trading_time
        except IndexError:
            print("Failed to parse trading time from message:", message)
            return None


    @staticmethod
    def generate_random_id(length=8):
        # Generates a random string of uppercase letters and digits
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

    def handle_user_input(self):
        commands = {
            '1': ('Place Order', self.place_order),
            '2': ('Cancel Order', self.cancel_order),
            '3': ('Retrieve Order Book', self.retrieve_order_book),
            '4': ('Retrieve Executed Trades', self.retrieve_executed_trades),
            '5': ('Search Order', self.search_order),
        }

        while True:
            print("Commands:")
            for num, (desc, _) in commands.items():
                print(f"{num}. {desc}")

            user_command = input("Enter command number: ")

            if user_command not in commands:
                print("Invalid command number. Try again.")
                continue

            trading_pair = ''
            if user_command in ['1', '3', '4']:  # commands that require a trading pair
                trading_pair = input("Enter trading pair (BTCUSDT or ETHUSDT): ").upper()
                if trading_pair not in ["BTCUSDT", "ETHUSDT"]:
                    print("Invalid trading pair. Try again.")
                    continue
                self.trading_pair = trading_pair
                
            if user_command == '1':  # Place Order
                order_details = input("Enter order details (Price=1671;Qty=0.5;Side=Buy): ")
                commands[user_command][1](f"{trading_pair};{order_details}")
            elif user_command == '2':  # Cancel Order
                order_id = input("Enter order ID to cancel: ")
                commands[user_command][1](f"{order_id}")
            elif user_command == '5':  # Search Order
                order_id = self.sender_comp_id
                commands[user_command][1](order_id)
            else:
                commands[user_command][1](trading_pair)


    def format_message(self, msg_type: str, fields: Dict[str, str]) -> str:
        """
        Format a message according to the specified rules.
        """
        msg = f"{msg_type};"
        msg += ';'.join(f"{key}={value}" for key, value in fields.items())
        return msg

    def parse_message(self, msg: str) -> Dict[str, str]:
        """
        Parse a message according to the specified rules.
        """
        segments = msg.split(';')
        fields = {segment.split('=')[0]: segment.split('=')[1] for segment in segments if '=' in segment}
        return fields


    def place_order(self, order_details):
        try:
            # Splitting the user input into individual details
            details = order_details.split(';')
            
            # Assuming order_details are given in the format: "Price=100;Qty=10;Side=Buy"
            fields = {detail.split('=')[0]: detail.split('=')[1] for detail in details if '=' in detail}
            order_id = TradingClient.order_id_counter
            TradingClient.order_id_counter += 1
            # Constructing the message string based on the given format
            order_message = (
                f"0;"
                f"35=D;"  # Message type for new order
                f"49={self.sender_comp_id};"   # Placeholder for sender comp ID, replace 'EXCHANGE' with actual value if necessary
                f"37={order_id};"  # Placeholder for order ID, replace 'some_id' with actual value if necessary
                f"38={fields['Qty']};"  # Quantity
                f"40=2;"  # Order type, assuming '2' is the desired value
                f"44={fields['Price']};"  # Price
                f"52={self.trading_time};"  # Placeholder for sending time, replace with actual value if necessary
                f"54={'1' if fields['Side'].lower() == 'buy' else '2'};"  # Side, assuming '1' for Buy and '2' for Sell
                f"6404=0.0;"  # Placeholder for POV target percentage, replace with actual value if necessary
                f"55={self.trading_pair}"  # Trading pair
            )
            
            print(f"Sending order: {order_message}")
            self.trading_pair = '' # Resetting trading pair
            
            self.order_publisher.send_string(order_message)
        except ValueError as e:
            print(f"Error: {e}. Please ensure that order details are formatted correctly.")
            
    def cancel_order(self, order_id):
        fields = {"37": order_id}  # Assuming "37" is the key for OrderID
        cancel_message = self.format_message("1", fields)  # Assuming msg_type "1" for cancel orders
        print(f"Sending cancel: {cancel_message}")
        self.order_publisher.send_string(cancel_message)

    def retrieve_order_book(self, trading_pair):
        request_message = f"2;order_book;{trading_pair}"  # Adding trading pair to the request message
        print(f"Sending order book request: {request_message}")
        self.order_publisher.send_string(request_message)

    def retrieve_executed_trades(self, trading_pair):
        request_message = f"3;executed_trades;{trading_pair}"  # Adding trading pair to the request message
        print(f"Sending executed trades request: {request_message}")
        self.order_publisher.send_string(request_message)

    def search_order(self, order_id):
        request_message = f"5;search_order;{order_id}"
        print(f"Sending search order request: {request_message}")
        self.order_publisher.send_string(request_message)
        
    def listen_for_acks(self):
        while True:
            ack_message = self.ack_subscriber.recv_string()
            fields = self.parse_message(ack_message)
            print(f"Received Ack: {fields}")

            if fields.get("35") == "order_book":  # Assuming msg_type "order_book" for order book updates
                order_book = json.loads(fields.get("data", "{}"))
                print(f"Order Book: {order_book}")
            elif fields.get("35") == "executed_trades":  # Assuming msg_type "executed_trades" for executed trades updates
                executed_trades = json.loads(fields.get("data", "{}"))
                print(f"Executed Trades: {executed_trades}")
            elif fields.get("35") == "search_order":  # Assuming msg_type "search_order" for search order responses
                order_data = json.loads(fields.get("data", "{}"))
                print(f"Order Data: {order_data}")

if __name__ == "__main__":
    client = TradingClient()

# poetry run python client.py