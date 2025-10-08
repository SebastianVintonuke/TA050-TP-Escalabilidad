
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
    def __init__(self,headers_obj):
        super().__init__(headers_obj)
    
    def add_row(self,row):
        self.payload.append(",".join(map(str, row)))
    def add_row_bytes(self,row):
        self.payload.append((b",".join(row)).decode())

    def add_row_dict(self,row):
        self.add_row_vec(row.values())
    def clone(self):
        return CSVMessageBuilder(self.headers.clone())

class CSVHashedMessageBuilder(HashedMessageBuilder):
    def __init__(self,headers_obj, key_hash):
        super().__init__(headers_obj, key_hash)

    def add_row(self,row):
        self.payload.append(",".join(map(str, row)))
    def add_row_bytes(self,row):
        self.payload.append((b",".join(row)).decode())

    def add_row_dict(self,row):
        self.add_row_vec(row.values())

    def clone(self):
        return CSVHashedMessageBuilder(self.headers.clone(), self.key_hash)


def builder_to_msg(builder):
    return CSVMessageBuilder([msg.ids[ind]], [msg.types[ind]], msg.partition)



def csv_msg_from_msg(msg, ind):
    return CSVMessageBuilder([msg.ids[ind]], [msg.types[ind]], msg.partition)

def csv_hashed_from_msg(msg, ind):
    return CSVHashedMessageBuilder([msg.ids[ind]], [msg.types[ind]], msg.ids[ind]+str(msg.types[ind]), msg.partition)

def msg_from_credentials(uuid, type, partition):
    return CSVMessageBuilder([uuid], [type], partition)


def hashed_msg_from_credentials(uuid, type,partition):
    return CSVHashedMessageBuilder([uuid], [type], str(uuid)+str(type), partition)
