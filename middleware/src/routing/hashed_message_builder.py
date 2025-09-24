
from .header_fields import *
# Message builder
class HashedMessageBuilder:
    def __init__(self,queries_id, queries_type, key_hash):
        self.ids = queries_id
        self.types = queries_type
        self.payload = []
        self.key_hash = key_hash

    def add_to_key_hash(self, string):
        self.key_hash+= string

    def add_row(self,row):
        #assert len(row) == len(fields) # Same size of fields 
        self.payload.append(str(row))


    def serialize_payload(self):
        return ("\n".join(self.payload)).encode()

    def get_headers(self):
        return {
            FIELD_QUERY_ID: self.ids,
            FIELD_QUERY_TYPE: self.types,
        }


    def hash_in(self, count):
        h = hashlib.sha256(self.key_hash.encode()).hexdigest()
        return int(h, 16) % count