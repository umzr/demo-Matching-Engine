import zmq
import threading
import json

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
        while True:
            user_command = input("Enter command (place/cancel/book/trades/search): ")
            trading_pair = input("Enter trading pair (btc/usdt or eth/usdt): ").lower()
            if trading_pair not in ["btc/usdt", "eth/usdt"]:
                print("Invalid trading pair. Try again.")
                continue

            if user_command == "place":
                order_details = input("Enter order details: ")
                self.place_order(f"{trading_pair};{order_details}")
            elif user_command == "cancel":
                order_id = input("Enter order ID to cancel: ")
                self.cancel_order(f"{trading_pair};{order_id}")
            elif user_command == "book":
                self.retrieve_order_book(trading_pair)
            elif user_command == "trades":
                self.retrieve_executed_trades(trading_pair)
            elif user_command == "search":
                order_id = input("Enter order ID to search: ")
                self.search_order(order_id)
            else:
                print("Unknown command. Try again.")

    def search_order(self, order_id):
        request_message = f"4;search_order;{order_id}"
        print(f"Sending search order request: {request_message}")
        self.order_publisher.send_string(request_message)
        
    def place_order(self, order_details):
        order_message = f"0;{order_details}"  # Assuming a simplistic message format
        print(f"Sending order: {order_message}")
        self.order_publisher.send_string(order_message)

    def cancel_order(self, order_id):
        cancel_message = f"1;{order_id}"  # Assuming a simplistic message format
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

    def listen_for_acks(self):
        while True:
            ack_message = self.ack_subscriber.recv_string()

            print(f"Received Ack: {ack_message}")
            if ack_message.startswith("order_book"):
                order_book_data = ack_message.split(';', 1)[1]
                order_book = json.loads(order_book_data)
                print(f"Order Book: {order_book}")
            elif ack_message.startswith("executed_trades"):
                executed_trades_data = ack_message.split(';', 1)[1]
                executed_trades = json.loads(executed_trades_data)
                print(f"Executed Trades: {executed_trades}")
            elif ack_message.startswith("search_order"):
                search_order_data = ack_message.split(';', 1)[1]
                order_data = json.loads(search_order_data)
                print(f"Order Data: {order_data}")

if __name__ == "__main__":
    client = TradingClient()
