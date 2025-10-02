
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
        res = {
            FIELD_QUERY_ID: self.ids,
        }

        if len(self.types) >0 and self.types[0] != DEFAULT_QUERY_TYPE:
            res[FIELD_QUERY_TYPE] = self.types

        if self.partition_ind != DEFAULT_PARTITION_VALUE: # If it is the default one, then save it.
            res[FIELD_PARTITION_IND]= self.partition_ind

        return res

    def set_as_eof(self, count: int = 0):
        self.should_be_eof = True
        self.partition_ind = count

    def is_eof(self):
        return self.should_be_eof

    def set_error(self, code = DEFAULT_ERROR_SIGNAL):
        self.set_as_eof(DEFAULT_ERROR_SIGNAL if code >=EOF_SIGNAL else code)

    def clone(self):
        return MessageBuilder(self.ids, self.types, self.partition_ind)


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