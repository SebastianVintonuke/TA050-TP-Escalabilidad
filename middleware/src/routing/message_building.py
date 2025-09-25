
from .header_fields import *
from hashlib import sha256 as hash_function 

# Message builder
class MessageBuilder:
    def __init__(self,queries_id, queries_type, partition = 0):
        self.ids = queries_id
        self.types = queries_type
        self.payload = []
        self.partition_ind = partition
        self.should_be_eof = False

    def add_row(self,row):
        #assert len(row) == len(fields) # Same size of fields 
        self.payload.append(str(row))


    def serialize_payload(self):
        if self.should_be_eof:
            return b""

        return ("\n".join(self.payload)).encode()

    def get_headers(self):
        return {
            FIELD_QUERY_ID: self.ids,
            FIELD_QUERY_TYPE: self.types,
            FIELD_PARTITION_IND: self.partition_ind,
        }

    def set_as_eof(self):
        self.should_be_eof = True

    def set_finish_signal(self):
        self.set_as_eof()
        self.partition_ind = EOF_SIGNAL

    def set_error(self, code = DEFAULT_ERROR_SIGNAL):
        self.set_as_eof()
        if code >=EOF_SIGNAL:
            self.partition_ind = DEFAULT_ERROR_SIGNAL
        else:
            self.partition_ind = code



# HashedMessage Builder, add key hash methods
class HashedMessageBuilder(MessageBuilder):
    def __init__(self,queries_id, queries_type, key_hash, partition = 0):
        super().__init__(queries_id, queries_type, partition)
        self.key_hash = key_hash

    def add_to_key_hash(self, string):
        self.key_hash+= string

    def hash_in(self, count):
        h = hash_function(self.key_hash.encode()).hexdigest()
        return int(h, 16) % count