
from .header_fields import *
from hashlib import sha256 as hash_function 

# Message builder
class MessageBuilder:
    def __init__(self,headers_obj : BaseHeaders):
        self.headers = headers_obj
        self.payload = []
        self.should_be_eof = headers_obj.is_eof()

        #self.ids = queries_id
        #self.types = queries_type
        #self.partition_ind = partition

    def add_row(self,row):
        #assert len(row) == len(fields) # Same size of fields 
        self.payload.append(str(row))

    def has_payload(self):
        return len(self.payload) > 0

    def len_payload(self):
        return len(self.payload)

    def clear_payload(self):
        self.payload = []

    def serialize_payload(self):
        if self.should_be_eof or len(self.payload) == 0:
            return b""

        return ("\n".join(self.payload)).encode()

    def get_headers(self):
        return self.headers.to_dict()

    def set_as_eof(self, count: int = 0):
        self.should_be_eof = True
        self.headers.msg_count = count

    def is_eof(self):
        return self.should_be_eof

    def set_error(self, code = DEFAULT_ERROR_SIGNAL):
        self.set_as_eof(DEFAULT_ERROR_SIGNAL if code >=EOF_SIGNAL else code)

    def clone(self):
        return MessageBuilder(self.headers.clone())


# HashedMessage Builder, add key hash methods
class HashedMessageBuilder(MessageBuilder):
    def __init__(self,headers_obj, key_hash):
        super().__init__(headers_obj)
        self.key_hash = key_hash

    def add_to_key_hash(self, string):
        self.key_hash+= string

    def hash_in(self, count):
        h = hash_function(self.key_hash.encode()).hexdigest()
        return int(h, 16) % count

    def clone(self):
        return HashedMessageBuilder(self.headers.clone(), self.key_hash)
