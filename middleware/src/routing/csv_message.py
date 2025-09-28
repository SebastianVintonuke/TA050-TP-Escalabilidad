
from .csv_payload_deserializer import *
from .channel_message import *
from .message_building import *
import logging

def not_none(itm):
    return itm != None


def map_all_ints(row):
    try:
        return [int(x) for x in row]
    except Exception as e:
        logging.error(f"Failed deserial of row {row} invalid {e}")
        return None

class CSVMessage(ChannelMessage):
    def __init__(self, ch, method, headers, payload):
        super().__init__(ch, method, headers, payload)

    def _from_headers(self, headers):
        super()._from_headers(headers)
        # Check fields? eg: extra fields.

    def _deserialize_payload(self, payload): # Do nothing with it.
        return CSVPayloadDeserializer(payload)

    def clone_with(self, queries_id, queries_type):
        return CSVMessage(self.tag, queries_id, queries_type, self.payload)

    def stream_rows(self):
        return self.payload
    def map_stream_rows(self, map_func):
        return filter(not_none, map(map_func, self.payload)) # payload is already a stream, assumed only will be iterated once.

class CSVMessageBuilder(MessageBuilder):
    def __init__(self,queries_id, queries_type, partition = 0):
        super().__init__(queries_id, queries_type, partition)
    
    def add_row(self,row):
        self.payload.append(",".join(map(str, row)))
    def add_row_dict(self,row):
        self.add_row_vec(row.values())

class CSVHashedMessageBuilder(HashedMessageBuilder):
    def __init__(self,queries_id, queries_type, key_hash, partition = 0):
        super().__init__(queries_id, queries_type, key_hash, partition)

    def add_row(self,row):
        self.payload.append(",".join(map(str, row)))
    def add_row_dict(self,row):
        self.add_row_vec(row.values())


def csv_msg_from_msg(msg, ind):
    return CSVMessageBuilder([msg.ids[ind]], [msg.types[ind]], msg.partition)

def csv_hashed_from_msg(msg, ind):
    return CSVHashedMessageBuilder([msg.ids[ind]], [msg.types[ind]], msg.ids[ind], msg.partition)

def msg_from_credentials(uuid, type, partition):
    return CSVMessageBuilder([uuid], [type], partition)


def hashed_msg_from_credentials(uuid, type,partition):
    return CSVHashedMessageBuilder([uuid], [type], str(uuid)+str(type), partition)
