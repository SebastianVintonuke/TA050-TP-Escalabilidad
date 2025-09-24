
from .csv_payload_deserializer import *
from .channel_message import *
from .message import MessageBuilder
import logging

SELECT_FIELDS_QUERY_1 = ["year", "hour", "sum"]

def not_none(itm):
    return itm != None

def parse_select_task_body(row):
    try:
        row["year"] = int(row["year"])
        row["hour"] = int(row["hour"])
        row["sum"] = int(row["sum"])
        return row
    except Exception as e:
        logging.error(f"Failed deserial of row {row} invalid {e}")
        return None

class SelectTaskMessage(ChannelMessage):
    def __init__(self, ch, method, headers, payload):
        super().__init__(ch, method, headers, payload)

    def _from_headers(self, headers):
        super()._from_headers(headers)
        # Check fields?

    def _deserialize_payload(self, payload): # Do nothing with it.
        return CSVPayloadDeserializer(SELECT_FIELDS_QUERY_1, payload)

    def clone_with(self, queries_id, queries_type):
        return SelectTaskMessage(self.tag, queries_id, queries_type, self.payload)

    def stream_rows(self):
        return filter(not_none, map(parse_select_task_body, self.payload)) # payload is already a stream, assumed only will be iterated once.

class SelectTaskMessageBuilder(MessageBuilder):
    def __init__(self,queries_id, queries_type):
        super().__init__(queries_id, queries_type)
    def add_row(self,row):
        vls = []
        for itm in SELECT_FIELDS_QUERY_1: # Just to make it more expressive let it be a {"year": vl}
            vls.append(str(int(row[itm])))

        # Check all of fields are there first
        self.payload.append(",".join(vls))

    def serialize_payload(self):
        return ("\n".join(self.payload)).encode()


def select_task_from_msg(msg, ind):
    return SelectTaskMessageBuilder([msg.ids[ind]], [msg.types[ind]])