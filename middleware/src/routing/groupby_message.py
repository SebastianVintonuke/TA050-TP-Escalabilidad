
from .csv_payload_deserializer import *
from .channel_message import *
from .message_building import HashedMessageBuilder
import logging
import hashlib


GROUP_BY_FIELDS_1 = ["year", "hour", "sum"]

def not_none(itm):
    return itm != None

def parse_groupby_body(row):
    try:
        row["year"] = int(row["year"])
        row["hour"] = int(row["hour"])
        row["sum"] = int(row["sum"])
        return row
    except Exception as e:
        logging.error(f"Failed deserial of row {row} invalid {e}")
        return None

class GroupbyMessage(ChannelMessage):
    def __init__(self, ch, method, headers, payload):
        super().__init__(ch, method, headers, payload)

    def _from_headers(self, headers):
        super()._from_headers(headers)
        # Check fields?

    def _deserialize_payload(self, payload): # Do nothing with it.
        return CSVPayloadDeserializer(GROUP_BY_FIELDS_1, payload)

    def clone_with(self, queries_id, queries_type):
        return GroupbyMessage(self.tag, queries_id, queries_type, self.payload)

    def stream_rows(self):
        return filter(not_none, map(parse_groupby_body, self.payload)) # payload is already a stream, assumed only will be iterated once.

class GroupbyMessageBuilder(HashedMessageBuilder):
    def __init__(self,queries_id, queries_type, key_hash, partition = 0):
        super().__init__(queries_id, queries_type, key_hash, partition)

    def add_row(self,row):
        vls = []
        for itm in GROUP_BY_FIELDS_1: # Just to make it more expressive let it be a {"year": vl}
            vls.append(str(int(row[itm])))

        # Check all of fields are there first
        self.payload.append(",".join(vls))


def result_from_msg(msg, ind):
    return GroupbyMessageBuilder([msg.ids[ind]], [msg.types[ind]], msg.ids[ind])