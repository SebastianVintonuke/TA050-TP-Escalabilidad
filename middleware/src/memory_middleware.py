from middleware.middleware import *
from middleware.routing.csv_message import *

from middleware.routing.message_building import *
from middleware.routing.message import *

import logging


class MemoryMessage(Message):
    def __init__(self, payload):
        super().__init__(payload)

class HashedMemoryMessageBuilder(HashedMessageBuilder):

    def with_credentials(query_id, q_type):
        return HashedMemoryMessageBuilder(BaseHeaders([query_id], [q_type]), query_id)

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
        return HashedMemoryMessageBuilder(self.headers.clone(), self.key_hash)

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

class SerializeMemoryMiddleware(MemoryMiddleware):
    def send(self, builder):
        self.listener(builder.headers, builder.serialize_payload())