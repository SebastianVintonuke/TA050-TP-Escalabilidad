

import csv
import io

class CSVPayloadDeserializer:
    def __init__(self,  byte_stream):
        byte_stream = io.BytesIO(byte_stream) # Its b"dd" received, make it a stream.

        self.reader = csv.reader(io.TextIOWrapper(byte_stream, encoding='utf-8'))

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.reader) # Reads up to \n
        return row
