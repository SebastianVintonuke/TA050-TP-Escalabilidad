

import csv
import io

class CSVPayloadDeserializer:
    def __init__(self, fields, byte_stream):
        self.fields = fields
        
        byte_stream = io.BytesIO(byte_stream) # Its b"dd" received, make it a stream.

        self.reader = csv.reader(io.TextIOWrapper(byte_stream, encoding='utf-8'))

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader)
        what_got= dict(zip(self.fields, row))
        return what_got
