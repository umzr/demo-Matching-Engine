import zmq
import threading
import json
from typing import Dict

class TradingClient:
    def __init__(self):
        self.context = zmq.Context()

        self.order_publisher = self.context.socket(zmq.PUB)
        self.order_publisher.connect("tcp://127.0.0.1:5557")

        self.ack_subscriber = self.context.socket(zmq.SUB)
        self.ack_subscriber.connect("tcp://127.0.0.1:5558")
        self.ack_subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

        self.user_input_thread = threading.Thread(target=self.handle_user_input)
        self.user_input_thread.start()

        self.listen_for_acks()

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
            if user_command in ['1', '2', '3', '4']:  # commands that require a trading pair
                trading_pair = input("Enter trading pair (BTCUSDT or ETHUSDT): ").upper()
                if trading_pair not in ["BTCUSDT", "ETHUSDT"]:
                    print("Invalid trading pair. Try again.")
                    continue

            if user_command == '1':  # Place Order
                order_details = input("Enter order details: ")
                commands[user_command][1](f"{trading_pair};{order_details}")
            elif user_command == '2':  # Cancel Order
                order_id = input("Enter order ID to cancel: ")
                commands[user_command][1](f"{trading_pair};{order_id}")
            elif user_command == '5':  # Search Order
                order_id = input("Enter order ID to search: ")
                commands[user_command][1](order_id)
            else:
                commands[user_command][1](trading_pair)


    def format_message(self, msg_type: str, fields: Dict[str, str]) -> str:
        """
        Format a message according to the specified rules.
        """
        msg = f"35={msg_type};"
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
            fields = dict(item.split('=') for item in order_details.split(';') if '=' in item)
            order_message = self.format_message("0", fields)  # Assuming msg_type "0" for new orders
            print(f"Sending order: {order_message}")
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
        request_message = f"4;search_order;{order_id}"
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