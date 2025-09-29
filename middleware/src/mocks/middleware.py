from middleware.routing.message import *

from middleware.middleware import *
from middleware.routing.message_building import *
from middleware.routing.header_fields import *


class MockMiddleware(MessageMiddleware):
    def __init__(self):
        self.msgs = []
        self.callback = None

    def send(self, msg):
        self.msgs.append(msg)

    def push_msg(self, msg):
        self.callback(msg)

    def start_consuming(self, on_message_callback):
        self.callback = on_message_callback

    def stop_consuming(self):
        pass

    def close(self):
        pass

    def delete(self):
        pass

class MockCopyMiddleware(MockMiddleware):
    def send(self, msg):
        cloned = msg.clone()
        cloned.payload = [itm for itm in msg.payload]
        super().send(cloned)


class MockMessageBuilder(HashedMessageBuilder):
    def __init__(self, msg, ind):
        super().__init__([], [], "key_hash", msg.partition)
        self.msg_from = msg
        self.ind = ind
        self.payload = []

    def add_row(self, row):
        # assert len(row) == len(fields) # Same size of fields
        self.payload.append(row)

    def clone(self):
        msg = MockMessageBuilder(self.msg_from, self.ind)
        msg.partition_ind = self.partition_ind
        msg.should_be_eof = self.should_be_eof
        return msg


class BareMockMessageBuilder(HashedMessageBuilder):
    def __init__(self, msg, ind):
        super().__init__([], [], "key_hash", 0)
        self.payload = []

    def add_row(self, row):
        # assert len(row) == len(fields) # Same size of fields
        self.payload.append(row)

    def clone(self):
        msg = BareMockMessageBuilder(None, 0)
        msg.partition_ind = self.partition_ind
        msg.should_be_eof = self.should_be_eof
        return msg



class MockMessage(Message):
    def __init__(self, tag, queries_id, queries_type, payload, map_to_vec):
        super().from_data(tag, queries_type, queries_type, payload)
        self.map_to_vec = map_to_vec

    def _deserialize_payload(self, payload):  # Do nothing with it.
        return payload

    def clone_with(self, queries_id, queries_type):
        return MockMessage(self.tag, queries_id, queries_type, self.payload)

    def stream_rows(self):
        return map(self.map_to_vec, iter(self.payload))
        

    def set_partition_eof(self):
        self.payload = None

    def set_error(self, code):
        self.set_partition_eof()
        self.partition =code # Negative partition es eof, be it an error or actual eof.

    def set_eof(self):
        self.set_partition_eof()
        self.partition = EOF_SIGNAL