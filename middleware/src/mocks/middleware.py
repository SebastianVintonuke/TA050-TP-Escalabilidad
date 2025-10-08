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
        self.callback(msg.headers, msg.payload)

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

    def default():
        return MockMessageBuilder(None, -1)
    def __init__(self, msg, ind):
        super().__init__(BaseHeaders.default(), "")
        self.msg_from = msg
        self.ind = ind

    def add_row(self, row):
        # assert len(row) == len(fields) # Same size of fields
        self.payload.append(row)

    def clone(self):
        msg = MockMessageBuilder(self.msg_from, self.ind)
        msg.headers = self.headers.clone()
        return msg


class BareMockMessageBuilder(HashedMessageBuilder):
    def default():
        return BareMockMessageBuilder(BaseHeaders.default())

    def creator_with_type(new_type):
        def converter(headers):
            headers.types[0] =new_type
            return BareMockMessageBuilder(headers)

        return converter    

    def for_payload(ids, types, rows, mapper):
        res = BareMockMessageBuilder(BaseHeaders(ids, types))
        
        for row in rows:
            res.add_row(mapper(row))

        return res

    def __init__(self, headers):
        super().__init__(headers, "")
    def add_row(self, row):
        self.payload.append(row)

    def clone(self):
        return BareMockMessageBuilder(self.headers.clone())



def identity(itm):
    return itm

class MockMessage(Message):

    def __init__(self, payload, map_to_vec= identity):
        super().__init__([map_to_vec(itm) for itm in payload])

    def _set_eof(self):
        self.empty = True
        self.payload = []

    def set_error(self, code):
        self._set_eof()
        self.partition =code # Negative partition es eof, be it an error or actual eof.

    def set_as_eof(self, count= 1):
        self._set_eof()
        self.partition = count