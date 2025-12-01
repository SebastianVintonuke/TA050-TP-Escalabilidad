
from .csv_payload_deserializer import *
from .message import *
from .message_building import *
import logging

class CSVMessage(Message):
    def deserialize_payload(payload): # Do nothing with it.
        if len(payload) == 0:
            return None # Empty payload == None == eof signal
        return CSVPayloadDeserializer(payload)

    def __init__(self, payload):
        super().__init__(CSVMessage.deserialize_payload(payload))


class CSVMessageBuilder(MessageBuilder):

    def with_credentials(ids, types, packet_id = UNDEFINED_PACKET_ID):
        return CSVMessageBuilder(BaseHeaders(ids, types, packet_id = packet_id))

    def creator_with_type(new_type):
        def converter(headers):
            return CSVMessageBuilder(headers.with_types([new_type]))

        return converter

    def creator_with_types(*types):
        def converter(headers):
            return CSVMessageBuilder(headers.with_types_pad(list(types)))

        return converter


    def __init__(self,headers_obj):
        super().__init__(headers_obj)
    
    def add_row(self,row):
        self.payload.append(",".join(map(str, row)))
    def add_row_bytes(self,row):
        self.payload.append((b",".join(row)).decode())

    def clone(self):
        return CSVMessageBuilder(self.headers.clone())

class CSVHashedMessageBuilder(HashedMessageBuilder):
    def with_credentials(ids, types, key_hash, packet_id = UNDEFINED_PACKET_ID):
        return CSVHashedMessageBuilder(BaseHeaders(ids, types, packet_id = packet_id), key_hash)

    def creator_with_type(new_type):
        def converter(headers):
            return CSVHashedMessageBuilder(headers.with_types([new_type]), headers.ids[0])
        return converter

    def creator_with_types(*types):
        def converter(headers):
            return CSVHashedMessageBuilder(headers.with_types_pad(list(types)), headers.ids[0])
        return converter


    def simple_creator():
        def converter(headers):
            return CSVHashedMessageBuilder(headers, headers.ids[0])
        return converter


    def __init__(self,headers_obj, key_hash):
        super().__init__(headers_obj, key_hash)

    def add_row(self,row):
        self.payload.append(",".join(map(str, row)))
    def add_row_bytes(self,row):
        self.payload.append((b",".join(row)).decode())


    def clone(self):
        return CSVHashedMessageBuilder(self.headers.clone(), self.key_hash)



class CSVTypeHashedMessageBuilder(HashedMessageBuilder):
    def with_credentials(ids, types, key_hash, packet_id = UNDEFINED_PACKET_ID):
        return CSVTypeHashedMessageBuilder(BaseHeaders(ids, types, packet_id = packet_id), key_hash)

    def creator_with_type(new_type):
        def converter(headers):
            return CSVTypeHashedMessageBuilder(headers.with_types([new_type]), headers.ids[0])
        return converter

    def creator_with_types(*types):
        def converter(headers):
            return CSVTypeHashedMessageBuilder(headers.with_types_pad(list(types)), headers.ids[0])
        return converter


    def simple_creator():
        def converter(headers):
            return CSVTypeHashedMessageBuilder(headers, headers.ids[0])
        return converter


    def __init__(self,headers_obj, key_hash):
        super().__init__(headers_obj, key_hash)
    def clone(self):
        return CSVTypeHashedMessageBuilder(self.headers.clone(), self.key_hash)

    def add_row(self,row):
        self.payload.append(",".join(map(str, row)))
    def add_row_bytes(self,row):
        self.payload.append((b",".join(row)).decode())

