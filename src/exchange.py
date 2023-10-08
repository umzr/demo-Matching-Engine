from ast import List
from collections import deque
import json
import math
import shutil
import zmq
import time
from typing import List, Union
from tabulate import tabulate


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class Order:
    def __init__(self, msg_type: str, order_id: str, order_qty: float, ord_type: str, price: float,
                 sender_comp_id: str, sending_time: int, side: str, pov_target_percentage: float, trading_pair: str):
        self.MsgType = msg_type
        self.OrderID = order_id
        self.OrderQty = order_qty
        self.OrdType = ord_type
        self.Price = price
        self.SenderCompID = sender_comp_id
        self.SendingTime = sending_time
        self.Side = side
        self.POVTargetPercentage = pov_target_percentage
        self.TradingPair = trading_pair  # Added TradingPair attribute

    @classmethod
    def from_string(cls, msg: str):
        segments = msg.split(';')
        fields = {segment.split('=')[0]: segment.split('=')[1] for segment in segments if '=' in segment}
        try:
            sending_time = int(fields['52'])
        except ValueError:
            raise ValueError(f"Expected integer for sending_time, got: {fields['52']}")
        # ... repeat for other fields you expect to convert ...
        return cls(
            fields['35'], fields['37'], float(fields['38']), fields['40'], float(fields['44']), fields['49'],
            sending_time, fields['54'], float(fields['6404']), fields['55']  # Added trading_pair
        )

    def to_string(self) -> str:
        return f"35={self.MsgType};49={self.SenderCompID};37={self.OrderID};38={self.OrderQty};" \
               f"40={self.OrdType};44={self.Price};52={self.SendingTime};54={self.Side};" \
               f"6404={self.POVTargetPercentage};55={self.TradingPair}"  # Added trading_pair

    def to_dict(self):
        return {
            'MsgType': self.MsgType,
            'OrderID': self.OrderID,
            'OrderQty': self.OrderQty,
            'OrdType': self.OrdType,
            'Price': self.Price,
            'SenderCompID': self.SenderCompID,
            'SendingTime': self.SendingTime,
            'Side': self.Side,
            'POVTargetPercentage': self.POVTargetPercentage,
            'TradingPair': self.TradingPair  # Added TradingPair
        }
class Ack:
    def __init__(self, target_comp_id: str, msg_type: str, order_id: str, order_qty: float, price: float):
        self.TargetCompID = target_comp_id
        self.MsgType = msg_type
        self.OrderID = order_id
        self.OrderQty = order_qty
        self.Price = price

    @classmethod
    def from_string(cls, msg: str):
        segments = msg.split(';')
        fields = {int(segment.split('=')[0]): segment.split('=')[1] for segment in segments}
        return cls(fields[56], fields[35], fields[37], float(fields[38]), float(fields[44]))

    def to_string(self) -> str:
        return f"35={self.MsgType};56={self.TargetCompID};37={self.OrderID};38={self.OrderQty};44={self.Price}"
    

def parse_quotes(market_data: str, instrument: str) -> List[Order]:
    res = []
    segments = market_data.split(';')
    # print(f"Segments: {segments}")  # Debugging statement to print segments
    fields = {segment.split('=')[0]: segment.split('=')[1] for segment in segments if '=' in segment}
    # print(f"Fields: {fields}")  # Debugging statement to print fields
    
    try:
        bid_price = float(fields.get('best_bid_price', 'NaN'))  # Using the correct key 'best_bid_price'
        bid_qty = float(fields.get('best_bid_qty', 'NaN'))  # Using the correct key 'best_bid_qty'
        ask_price = float(fields.get('best_ask_price', 'NaN'))  # Using the correct key 'best_ask_price'
        ask_qty = float(fields.get('best_ask_qty', 'NaN'))  # Using the correct key 'best_ask_qty'
        transaction_time = int(fields.get('transaction_time', '0'))  # Using the correct key 'transaction_time'
        event_time = int(fields.get('event_time', '0'))  # Using the correct key 'event_time'
    except ValueError as ve:
        print(f"ValueError: {ve}, market_data: {market_data}")
        return res  # Return empty list if parsing fails

    if not (math.isnan(bid_price) or math.isnan(bid_qty)):
        bid_order = Order(
            msg_type="0",
            order_id=f"{instrument}_bid_{event_time}",  # Creating a unique order ID based on instrument and event_time
            order_qty=bid_qty,
            ord_type="2",
            price=bid_price,
            sender_comp_id="EXCHANGE",
            sending_time=transaction_time,
            side="1",  # 1 = Buy
            pov_target_percentage=0.0  # placeholder
        )
        res.append(bid_order)

    if not (math.isnan(ask_price) or math.isnan(ask_qty)):
        ask_order = Order(
            msg_type="0",
            order_id=f"{instrument}_ask_{event_time}",  # Creating a unique order ID based on instrument and event_time
            order_qty=ask_qty,
            ord_type="2",
            price=ask_price,
            sender_comp_id="EXCHANGE",
            sending_time=transaction_time,
            side="2",  # 2 = Sell
            pov_target_percentage=0.0  # placeholder
        )
        res.append(ask_order)

    return res

def rounding_off_float(val: float, precision: int = 10000) -> float:
    return round(val * precision) / precision


def send_all_messages(filled_orders: List[Order], publisher, log):
    for order in filled_orders:
        data = order.to_string()
        print(f"Strategy out: [[{'N' if order.MsgType == '0' else 'C'}{order.Price}:{order.OrderQty}]]")
        publisher.send_string(data)
        print(f"sent: {data}")


def send_all_messages_ack(acks: List[Ack], publisher, log, cumulative_quantity: float):
    for ack in acks:
        data = ack.to_string()
        if ack.MsgType == "3":
            cumulative_quantity += ack.OrderQty
            print(f"Filled: {ack.OrderQty}@{ack.Price}, Cumulative Quantity: {cumulative_quantity}")
        publisher.send_string(data)
        print(f"sent: {data}")

class BidAskQueue:
    def __init__(self):
        self.bid_queue = {}  # {instrument: deque()}
        self.ask_queue = {}  # {instrument: deque()}
        self.client_orders = []
        self.executed_trades = {}  # {instrument: []}

    def search_order(self, order_id):
        for instrument, queue in self.bid_queue.items():
            for order in queue:
                if order.OrderID == order_id:
                    return order, instrument, "bid"
        for instrument, queue in self.ask_queue.items():
            for order in queue:
                if order.OrderID == order_id:
                    return order, instrument, "ask"
        return None, None, None  # Return None values if order not found
    
    def insert_bid(self, instrument, ord: Order):
        if instrument not in self.bid_queue:
            self.bid_queue[instrument] = deque()
        self.bid_queue[instrument].append(ord)


    def record_trade(self, instrument, trade):
        if instrument not in self.executed_trades:
            self.executed_trades[instrument] = []
        self.executed_trades[instrument].append(trade)

    def get_executed_trades(self, instrument):
        return self.executed_trades.get(instrument, [])
    
    def get_order_book(self, instrument):
        bid_queue = self.bid_queue.get(instrument, deque())
        ask_queue = self.ask_queue.get(instrument, deque())
        return {
            "bids": [order.to_dict() for order in bid_queue],
            "asks": [order.to_dict() for order in ask_queue]
        }

    def print_order_book(self, instrument):
        order_book = self.get_order_book(instrument)
        print(f"Order Book for {instrument}:")
        print("Bids:")
        for bid in order_book["bids"]:
            print(f"Price: {bid['price']}, Quantity: {bid['qty']}")
        print("Asks:")
        for ask in order_book["asks"]:
            print(f"Price: {ask['price']}, Quantity: {ask['qty']}")
            
    def insert_ask(self, instrument, ord: Order):
        if instrument not in self.ask_queue:
            self.ask_queue[instrument] = deque()
        self.ask_queue[instrument].append(ord)

    def clear_bid(self):
        self.bid_queue.clear()

    def clear_ask(self):
        self.ask_queue.clear()

    def pop_bid(self) -> Union[Order, None]:
        return self.bid_queue.popleft() if self.bid_queue else None

    def pop_ask(self) -> Union[Order, None]:
        return self.ask_queue.popleft() if self.ask_queue else None

    def fill_orders(self, filled_orders: List[Ack]) -> bool:
        res = False
        for client in self.client_orders[:]:
            instrument = client.instrument  # Assuming 'instrument' attribute in Order class
            ask_queue_for_instrument = self.ask_queue.get(instrument, deque())  
            if not ask_queue_for_instrument:
                continue  
            for ask in ask_queue_for_instrument[:]:
                print(f"ask price: {ask.Price}")  
                if client.Price == ask.Price:
                    res = True
                    amount_filled = min(client.OrderQty, ask.OrderQty)
                    client.OrderQty -= amount_filled
                    print(f"Filled: {amount_filled} ,order: {client.to_string()}")  # Logging filled order
                    ack_message = Ack(client.SenderCompID, "4", client.OrderID, amount_filled, client.Price)
                    filled_orders.append(ack_message)
                if client.OrderQty == 0:
                    self.client_orders.remove(client)
                    break  # Order is fully filled, break out of the inner loop
        print(f"cur qty: {self.client_orders[0].OrderQty if self.client_orders else 'N/A'}, "  # Logging current qty
              f"askQueueSize: {sum(len(q) for q in self.ask_queue.values())}, "  # Total size of all ask queues
              f"clientOrderSize: {len(self.client_orders)}")  # Logging client order size
        return res


    def format_order_book(self, order_book):
        terminal_width, _ = shutil.get_terminal_size()
        half_width = terminal_width // 2

        bid_table = tabulate(order_book['bids'], headers='keys', tablefmt='plain', numalign="right")
        ask_table = tabulate(order_book['asks'], headers='keys', tablefmt='plain', numalign="right")

        bid_lines = bid_table.split('\n')
        ask_lines = ask_table.split('\n')

        max_lines = max(len(bid_lines), len(ask_lines))

        formatted_order_book = []
        for i in range(max_lines):
            bid_line = bid_lines[i] if i < len(bid_lines) else ''
            ask_line = ask_lines[i] if i < len(ask_lines) else ''
            formatted_order_book.append(f'{bid_line:{half_width}} | {ask_line:{half_width}}')

        return '\n'.join(formatted_order_book)
    
    def adding_quotes_into_queues(self, updt: str):
        # Assuming you have a method to generate unique order IDs
        self.order_counter = 0  

        # self.clear_bid()
        # self.clear_ask()
        parsed_str_list = updt.split(';')
        print(f'{bcolors.OKGREEN} parsed_str_list: {parsed_str_list} {bcolors.ENDC}')

        data_dict = {item.split('=')[0].replace('Q ', ''): item.split('=')[1] for item in parsed_str_list if '=' in item}

        instrument = data_dict.get('instrument', None)
        if instrument is None:
            print(f'{bcolors.FAIL}No instrument found{bcolors.ENDC}')
            return

        bid_price = data_dict.get('best_bid_price', None)
        bid_qty = data_dict.get('best_bid_qty', None)
        ask_price = data_dict.get('best_ask_price', None)
        ask_qty = data_dict.get('best_ask_qty', None)

        if bid_price is not None and bid_qty is not None:
            self.order_counter += 1  # Increment order_counter for a new order ID
            bid_order = Order(
                msg_type='D',
                order_id=str(self.order_counter),  # Use order_counter as order ID
                order_qty=float(bid_qty),
                ord_type='2',
                price=float(bid_price),
                sender_comp_id='EXCHANGE',
                sending_time=int(data_dict.get('transaction_time', 0)),
                side='1',
                pov_target_percentage=0.0,
                trading_pair=instrument  # Use instrument as trading_pair
            )
            self.insert_bid(instrument, bid_order)
            print(f"BID PARSER: {bid_order.to_string()}")

        if ask_price is not None and ask_qty is not None:
            self.order_counter += 1  # Increment order_counter for a new order ID
            ask_order = Order(
                msg_type='D',
                order_id=str(self.order_counter),  # Use order_counter as order ID
                order_qty=float(ask_qty),
                ord_type='2',
                price=float(ask_price),
                sender_comp_id='EXCHANGE',
                sending_time=int(data_dict.get('transaction_time', 0)),
                side='2',
                pov_target_percentage=0.0,
                trading_pair=instrument  # Use instrument as trading_pair
            )
            self.insert_ask(instrument, ask_order)
            print(f"ASK PARSER: {ask_order.to_string()}")



    def parse_quotes(self, quotes_str: str) -> List[Order]:
        # Assume each quote is separated by a comma for simplicity
        quotes_data = quotes_str.split(',')
        return [Order(*data.split()) for data in quotes_data]  # Assume each data field in a quote is separated by a space

    def cancel_order(self, cancel_request: Order, cancelled_orders: List[Ack]) -> bool:
        res = False
        for client in self.client_orders:
            if cancel_request.OrderID == client.OrderID:
                res = True
                ack_cancel_msg = Ack(cancel_request.SenderCompID, "5", cancel_request.OrderID, -1, client.Price)
                cancelled_orders.append(ack_cancel_msg)
                self.client_orders.remove(client)
                break
        return res

    def try_fill_3mins_order(self, filled_orders: List[Ack]) -> bool:
        res = False
        ms_unix_time_now = int(time.time() * 1000)
        for client in self.client_orders[:]:
            if ms_unix_time_now - client.SendingTime >= 180000:
                res = True
                ack_fill_msg = Ack(client.SenderCompID, "4", client.OrderID, client.OrderQty, client.Price)
                filled_orders.append(ack_fill_msg)
                self.client_orders.remove(client)
        return res
    
class TradeMatchingEngine:
    def __init__(self):
        self.bid_ask = BidAskQueue()

    def run(self):
        print("Starting Trade Matching Engine...")
        context = zmq.Context()
        subscriber = context.socket(zmq.SUB)
        subscriber.connect("tcp://127.0.0.1:5556")
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "Q")

        order_subscriber = context.socket(zmq.SUB)
        order_subscriber.bind("tcp://127.0.0.1:5557")
        order_subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

        ack_publisher = context.socket(zmq.PUB)
        ack_publisher.connect("tcp://127.0.0.1:5558")

        time.sleep(0.2)  # Equivalent to usleep(200000)

        while True:
            print("waiting...")
            update = subscriber.recv_string()
            # print(f"Received Market Msg: {update}")
            self.bid_ask.adding_quotes_into_queues(update)

            while True:
                try:
                    # print("Attempting to receive message...")
                    update = order_subscriber.recv_string(flags=zmq.NOBLOCK)
                    # print(f"Received Client Msg: {update}")
                    msg_type = update.split(';')[0]  # Assuming the first field is always the message type
                    ack_publisher.send_string(update)
                    if msg_type == "0":  # order
                        print(f"{bcolors.OKCYAN}is order: {update} {bcolors.ENDC}")
                        order_from_client = Order.from_string(update)
                        self.bid_ask.client_orders.append(order_from_client)
                        
                        ack_publisher.send_string(f"{bcolors.OKCYAN}{len(self.bid_ask.client_orders)} queued. {bcolors.ENDC}")
                        # TODO: send ack order msg
                        ack_order_msg = Ack(order_from_client.SenderCompID, "3", order_from_client.OrderID, -1, order_from_client.Price)
                        data = ack_order_msg.to_string()  # Assuming to_string method to serialize your message
                        ack_publisher.send_string(data)
                        
                    elif msg_type == '2':  # Order book request
                        # finish
                        print("is order book request")
                        trading_pair = update.split(';')[2]
                        order_book = self.bid_ask.get_order_book(trading_pair)
                        
                        formatted_order_book = self.bid_ask.format_order_book(order_book)  # Fixed line
                        print(formatted_order_book)  # print the formatted order book to the terminal
                        order_book_message = f"order_book;{json.dumps(order_book)}"
                        ack_publisher.send_string(order_book_message)
                    elif update.startswith("4;search_order"):  # Search order request
                        trading_pair, order_id = update.split(';')[2:4]
                        order, instrument, order_type = self.bid_ask.search_order(order_id)
                        if order:
                            order_data = {
                                "instrument": instrument,
                                "order_type": order_type,
                                # ... other order details ...
                            }
                            search_order_message = f"search_order;{json.dumps(order_data)}"
                            ack_publisher.send_string(search_order_message)
                        else:
                            ack_publisher.send_string(f"search_order;Order {order_id} not found")
                except zmq.Again:
                    break

            filled_orders = []
            self.bid_ask.try_fill_3mins_order(filled_orders)
            if self.bid_ask.fill_orders(filled_orders):
                ack_publisher.send_string(f"{bcolors.OKGREEN}Filled orders: {(filled_orders)} {bcolors.ENDC}")
                print("Order filled!")
            else:
                print("No order filled!")
            # TODO: send all messages
            # ... your logic to send all messages

if __name__ == "__main__":
    exchange = TradeMatchingEngine()
    exchange.run()

# poetry run python exchange.py     