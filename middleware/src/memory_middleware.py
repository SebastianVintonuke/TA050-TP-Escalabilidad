from middleware.middleware import *
from middleware.routing.csv_message import *

from middleware.routing.message_building import *
from middleware.routing.message import *

import logging



def not_none(itm):
    return itm != None

# Do nothing but let have the contract, for the ack...
class MemoryMessageChannel:
    def basic_ack(self, delivery_tag):
        pass
class MethodClass:
    def __init__(self, tag):
        self.delivery_tag = tag

class HashedMemoryMessageBuilder(HashedMessageBuilder):
    def __init__(self,queries_id, queries_type, key_hash, partition = 0):
        super().__init__(queries_id, queries_type, key_hash, partition)

    def add_row(self, row):
        # assert len(row) == len(fields) # Same size of fields
        self.payload.append(row)

    def clone(self):
        return HashedMemoryMessageBuilder(self.ids, self.types, self.key_hash, self.partition_ind)


class MemoryMessage(Message):
    def __init__(self,  headers, payload):
        super().__init__(headers, payload)

    def _deserialize_payload(self, payload): # Do nothing with it.
        return payload

    def stream_rows(self):
        return self.payload
    def map_stream_rows(self, map_func):
        return filter(not_none, map(map_func, self.payload)) # payload is already a stream, assumed only will be iterated once.

def build_memory_message_builder(uuid, type,partition = 0):
    return HashedMemoryMessageBuilder([uuid], [type], str(uuid)+str(type), partition)

def build_memory_messages_builder(uuids, types, hashed,partition = 0):
    return HashedMemoryMessageBuilder(uuids, types, hashed, partition)

def memory_builder_from_msg(msg, ind):
    return build_memory_message_builder(msg.ids[ind], msg.types[ind], msg.partition)



DEF_CHANNEL = MemoryMessageChannel()
DEF_METHOD = MethodClass("")
def builder_to_memory_msg(builder):
    logging.info(f"SENDING over memory middleware {builder.get_headers()} {len(builder.payload)} Not serializing?")
    return MemoryMessage(builder.get_headers(), builder.payload)

def csv_builder_to_msg(builder):
    logging.info(f"SENDING over memory middleware {builder.get_headers()} {len(builder.payload)}")
    return CSVMessage(DEF_CHANNEL, DEF_METHOD, builder.get_headers(), builder.serialize_payload())



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