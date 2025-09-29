from middleware.middleware import *
from middleware.routing.csv_message import *
import logging
# Do nothing but let have the contract, for the ack...
class MemoryMessageChannel:
    def basic_ack(self, delivery_tag):
        pass
class MethodClass:
    def __init__(self, tag):
        self.delivery_tag = tag

def csv_builder_to_msg(builder):
    logging.info(f"SENDING over memory middleware {builder.get_headers()} {len(builder.payload)}")
    return CSVMessage(MemoryMessageChannel(), MethodClass(""), builder.get_headers(), builder.serialize_payload())


class MemoryMiddleware(MessageMiddleware):
    def __init__(self, msg_creator = csv_builder_to_msg):
        self.listener = None
        self.msg_creator = msg_creator

    def send(self, builder):
        self.listener(self.msg_creator(builder))


    def start_consuming(self, on_message_callback):
        self.listener = on_message_callback

    def stop_consuming(self):
        self.listener = None

    def close(self):
        pass

    def delete(self):
        pass