from middleware.middleware import * 
from middleware.routing.message import * 


class MockMiddleware(MessageMiddleware):
    def __init__(self):
        self.msgs= []
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


class MockMessageBuilder:
    def __init__(self,msg, ind):
        self.msg_from = msg
        self.ind= ind
        self.payload = []
        self.fields= []

    def set_fields(self, fields):
        # set field names!
        self.fields = fields

    def add_row(self,row):
        #assert len(row) == len(fields) # Same size of fields 
        self.payload.append(row)

class MockMessage(Message):
    def __init__(self, tag, queries_id, queries_type, payload, map_to_vec):
        super().from_data(tag,queries_type,queries_type, payload)
        self.map_to_vec = map_to_vec
    def _deserialize_payload(self, payload): # Do nothing with it.
        return payload

    def clone_with(self, queries_id, queries_type):
        return MockMessage(self.tag, queries_id, queries_type, self.payload)

    def stream_rows(self):
        return map(self.map_to_vec, iter(self.payload))

