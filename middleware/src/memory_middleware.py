from middleware.middleware import *
from middleware.routing.csv_message import *

from middleware.routing.message_building import *
from middleware.routing.message import *

import logging


class MemoryMessage(Message):
    def __init__(self, payload):
        super().__init__(payload)

class HashedMemoryMessageBuilder(HashedMessageBuilder):

    def creator_with_type(new_type):
        def converter(headers):
            headers.types[0] = new_type
            return HashedMemoryMessageBuilder(headers, headers.ids[0])
        return converter

    def creator_with_types(*types):
        def converter(headers):
            headers.types = list(types)
            return HashedMemoryMessageBuilder(headers, headers.ids[0])
        return converter


    def simple_creator():
        def converter(headers):
            return HashedMemoryMessageBuilder(headers, headers.ids[0])
        return converter


    def default():
        return HashedMemoryMessageBuilder(BaseHeaders.default(), "")

    def __init__(self,headers_obj,key_hash):
        super().__init__(headers_obj, key_hash)

    def add_row(self, row):
        # assert len(row) == len(fields) # Same size of fields
        self.payload.append(row)

    def clone(self):
        return HashedMemoryMessageBuilder(self.headers_obj.clone(), self.key_hash)


def build_memory_message_builder(uuid, type,msg_count = DEFAULT_PARTITION_VALUE):
    return HashedMemoryMessageBuilder(
        BaseHeaders([uuid], [type], msg_count)
        , str(uuid)+str(type))

def build_memory_messages_builder(uuids, types, hashed,msg_count):
    return HashedMemoryMessageBuilder(
        BaseHeaders(uuids, types,msg_count),
        hashed)

def memory_builder_from_msg(headers, ind):
    return build_memory_message_builder(headers.ids[ind], headers.types[ind], headers.msg_count)



def builder_to_memory_msg(builder):
    logging.info(f"SENDING over memory middleware {builder.get_headers()} {len(builder.payload)} Not serializing?")
    return MemoryMessage(builder.get_headers(), builder.payload)


class MemoryMiddleware(MessageMiddleware):
    def __init__(self):
        self.listener = None

    def send(self, builder):
        self.listener(builder.headers, builder.payload)

    def start_consuming(self, on_message_callback):
        self.listener = on_message_callback

    def stop_consuming(self):
        self.listener = None

    def close(self):
        pass

    def delete(self):
        pass