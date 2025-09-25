
from .header_fields import *
from hashlib import sha256 as hash_function 

# Message builder
class MessageBuilder:
    def __init__(self,queries_id, queries_type, partition = 0):
        self.ids = queries_id
        self.types = queries_type
        self.payload = []
        self.partition_ind = partition

    def add_row(self,row):
        #assert len(row) == len(fields) # Same size of fields 
        self.payload.append(str(row))


    def serialize_payload(self):
        return ("\n".join(self.payload)).encode()

    def get_headers(self):
        return {
            FIELD_QUERY_ID: self.ids,
            FIELD_QUERY_TYPE: self.types,
            FIELD_PARTITION_IND: self.partition_ind,
        }



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