from ast import List
from collections import deque
import math
import zmq
import time
from typing import List, Union



class Order:
    def __init__(self, msg_type: str, order_id: str, order_qty: float, ord_type: str, price: float,
                 sender_comp_id: str, sending_time: int, side: str, pov_target_percentage: float):
        self.MsgType = msg_type
        self.OrderID = order_id
        self.OrderQty = order_qty
        self.OrdType = ord_type
        self.Price = price
        self.SenderCompID = sender_comp_id
        self.SendingTime = sending_time
        self.Side = side
        self.POVTargetPercentage = pov_target_percentage

    @classmethod
    def from_string(cls, msg: str):
        segments = msg.split(';')
        fields = {int(segment.split('=')[0]): segment.split('=')[1] for segment in segments}
        return cls(
            fields[35], fields[37], float(fields[38]), fields[40], float(fields[44]), fields[49],
            int(fields[52]), fields[54], float(fields[6404])
        )

    def to_string(self) -> str:
        return f"35={self.MsgType};49={self.SenderCompID};37={self.OrderID};38={self.OrderQty};" \
               f"40={self.OrdType};44={self.Price};52={self.SendingTime};54={self.Side};" \
               f"6404={self.POVTargetPercentage}"


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
    
def parse_quotes(market_data: str) -> List[Order]:
    res = []
    quote_data = market_data.split()
    for i in range(0, len(quote_data), 2):
        price, qty = map(float, quote_data[i:i + 2])
        market_quote = Order("0", "-1", qty, "-1", price, "-1",
                             int(time.time() * 1000), "BID", 0)
        res.append(market_quote)
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

    def insert_bid(self, instrument, ord: Order):
        if instrument not in self.bid_queue:
            self.bid_queue[instrument] = deque()
        self.bid_queue[instrument].appendleft(ord)
        self.bid_queue.appendleft(ord)

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
            "bids": list(bid_queue),
            "asks": list(ask_queue)
        }

    def insert_ask(self, ord: Order):
        self.ask_queue.append(ord)

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
        for ask in self.ask_queue:
            print(f"ask price: {ask.Price}")
        if self.client_orders:
            print(f"cur qty: {self.client_orders[0].OrderQty}, askQueueSize: {len(self.ask_queue)}, clientOrderSize: {len(self.client_orders)}")
        for client in self.client_orders[:]:
            for ask in self.ask_queue[:]:
                if client.Price == ask.Price:
                    res = True
                    amount_filled = min(client.OrderQty, ask.OrderQty)
                    client.OrderQty -= amount_filled
                    print(f"Filled :{amount_filled} ,order: {client.to_string()}")
                    ack_message = Ack(client.SenderCompID, "4", client.OrderID, amount_filled, client.Price)
                    filled_orders.append(ack_message)
            if client.OrderQty == 0:
                self.client_orders.remove(client)
        return res

    def adding_quotes_into_queues(self, updt: str):
        self.clear_bid()
        self.clear_ask()
        parsed_str_list = updt.split(';')
        for idx, parsed_str in enumerate(parsed_str_list):
            if idx == 2:
                bid_quotes = self.parse_quotes(parsed_str)
                for quote in bid_quotes:
                    self.insert_bid(quote)
                    print(f"BID PARSER: {quote.to_string()}")
            elif idx == 3:
                ask_quotes = self.parse_quotes(parsed_str)
                for quote in ask_quotes:
                    self.insert_ask(quote)
                    print(f"ASK PARSER: {quote.to_string()}")

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
            update = subscriber.recv_string()
            print(f"Received Market Msg: {update}")
            self.bid_ask.adding_quotes_into_queues(update)

            print("waiting...")
            while True:
                try:
                    update = order_subscriber.recv_string(flags=zmq.NOBLOCK)
                    print(f"Received Client Msg: {update}")
                    order_from_client = Order(update)  # Assuming Order constructor can parse your message
                    if order_from_client.msg_type == "0":  # order
                        print("is order")
                        self.bid_ask.client_orders.append(order_from_client)
                        print(f"{len(self.bid_ask.client_orders)} queued.")
                        # TODO: send ack order msg
                        ack_order_msg = Ack(order_from_client.sender_comp_id, "3", order_from_client.order_id, -1, order_from_client.price)
                        data = ack_order_msg.to_string()  # Assuming to_string method to serialize your message
                        ack_publisher.send_string(data)
                    else:
                        print("is not order")
                        cancelled_orders = []
                        self.bid_ask.cancel_order(order_from_client, cancelled_orders)
                        # TODO: send all messages
                        # ... your logic to send all messages
                except zmq.Again:
                    break

            filled_orders = []
            self.bid_ask.try_fill_3mins_order(filled_orders)
            if self.bid_ask.fill_orders(filled_orders):
                print("Order filled!")
            else:
                print("No order filled!")
            # TODO: send all messages
            # ... your logic to send all messages

if __name__ == "__main__":
    exchange = TradeMatchingEngine()
    exchange.run()

# poetry run python exchange.py     