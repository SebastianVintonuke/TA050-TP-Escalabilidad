from middleware.routing.message import *

from middleware.middleware import *
from middleware.routing.message_building import *
from middleware.routing.header_fields import *
import logging


class IntermediateMiddleware(MessageMiddleware):
    def __init__(self, inner_middleware):
        self.msgs = []
        self.inner_middleware = inner_middleware

    def send(self, msg):
        cloned = msg.clone()
        cloned.payload = [itm for itm in msg.payload]
        self.msgs.append(cloned)
        logging.debug(f"INTERME SENDING {cloned.headers} len: {len(cloned.payload)} len msg:{len(msg.serialize_payload())}")
        self.inner_middleware.send(msg);

    def ack_message(self, message):
        self.inner_middleware.ack_message(message)

    def start_consuming(self, on_message_callback):
        self.inner_middleware.start_consuming(on_message_callback)

    def stop_consuming(self):
        self.inner_middleware.stop_consuming()

    def close(self):
        self.inner_middleware.close()

    def delete(self):
        self.inner_middleware.delete()

    def push_msg(self, msg):
        self.inner_middleware.send(msg);




class MockMiddleware(MessageMiddleware):
    def __init__(self):
        self.msgs = []
        self.callback = None
    def ack_message(self, message):
        pass
        
    def send(self, msg):
        self.msgs.append(msg)

    def push_msg(self, msg):
        msg.headers.tag = "tag"        
        self.callback(msg.headers, msg.payload)

    def start_consuming(self, on_message_callback):
        self.callback = on_message_callback

    def stop_consuming(self):
        pass

    def close(self):
        pass

    def delete(self):
        pass


class MockMiddlewareTags(MockMiddleware):
    def __init__(self):
        super().__init__()
        self.acked_messages = []

    def ack_message(self, message):
        self.acked_messages.append(message)
        
    def send(self, msg):
        self.msgs.append(msg)

    def push_msg(self, msg, tag):
        msg.headers.tag = tag
        res = self.callback(msg.headers, msg.payload)
        if res == None: # None == auto ack!
            self.ack_message(tag)


class MockCopyMiddleware(MockMiddleware):
    def send(self, msg):
        cloned = msg.clone()
        cloned.payload = [itm for itm in msg.payload]
        
        super().send(cloned)


class MockMessageBuilder(HashedMessageBuilder):

    def default():
        return MockMessageBuilder(None, -1)
    def __init__(self, msg, ind):
        super().__init__(BaseHeaders.default(), "_1")
        self.msg_from = msg
        self.ind = ind

    def add_row(self, row):
        # assert len(row) == len(fields) # Same size of fields
        self.payload.append(row)

    def clone(self):
        msg = MockMessageBuilder(self.msg_from, self.ind)
        msg.key_hash = self.key_hash
        msg.headers = self.headers.clone()
        return msg


class BareMockMessageBuilder(HashedMessageBuilder):
    def default(packet_id = 0):
        return BareMockMessageBuilder(BaseHeaders(["id_1"], [], packet_id = packet_id), "")

    def creator_with_type(new_type):
        def converter(headers):
            return BareMockMessageBuilder(headers.with_types([new_type]), headers.ids[0])

        return converter    

    def for_payload(ids, types, rows, mapper, packet_id = 0):
        res = BareMockMessageBuilder(BaseHeaders(ids, types, packet_id = packet_id), ids[0])
        
        for row in rows:
            res.add_row(mapper(row))

        return res

    def __init__(self, headers, key_hash = None):
        super().__init__(headers, key_hash if key_hash else headers.ids[0])
    def add_row(self, row):
        self.payload.append(row)

    def clone(self):
        return BareMockMessageBuilder(self.headers.clone(), self.key_hash)




class BareMockMessageBuilderNoSerial(BareMockMessageBuilder):
    def default(packet_id = 0):
        return BareMockMessageBuilderNoSerial(BaseHeaders(["id_1"], [],packet_id = packet_id), "")

    def creator_with_type(new_type):
        def converter(headers):
            return BareMockMessageBuilderNoSerial(headers.with_types([new_type]), headers.ids[0])

        return converter    

    def for_payload(ids, types, rows, mapper, packet_id = 0):
        res = BareMockMessageBuilderNoSerial(BaseHeaders(ids, types, packet_id = packet_id), ids[0])
        
        for row in rows:
            res.add_row(mapper(row))

        return res

    def __init__(self, headers, key_hash = None):
        super().__init__(headers,key_hash if key_hash else headers.ids[0])

    def serialize_payload(self):
        return self.payload

    def clone(self):
        return BareMockMessageBuilderNoSerial(self.headers.clone(), self.key_hash)


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

    def set_as_eof(self):
        self._set_eof()
