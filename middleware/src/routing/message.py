import logging
from .header_fields import *

# Message builder
class MessageBuilder:
	def __init__(self,queries_id, queries_type, partition):
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


# Defines basic initialization and header management
class Message:
	def _verify_headers(self):
		# assert len of queries == len of types!
		assert len(self.ids) == len(self.types)

	def _from_headers(self, headers):
		self.ids = headers.get(FIELD_QUERY_ID, [])
		self.types = headers.get(FIELD_QUERY_TYPE, [])
		self.partition = headers.get(FIELD_PARTITION_IND, 0)
		self._verify_headers()

	def _deserialize_payload(self, payload):
		return payload

	def from_data(self, tag, queries_id, queries_type, payload, partition = 0):
		self.tag = tag
		self.ids = queries_id
		self.types = queries_type
		self.partition = partition
		self._verify_headers()
		self.payload = payload

	def __init__(self, tag, headers, payload):
		self.tag = tag
		self._from_headers(headers)
		if len(payload) == 0:
			self.payload = None
		else:
			self.payload = self._deserialize_payload(payload)


	def len_queries(self):
		return len(self.ids)

	def clone_with(self, queries_id, queries_type):
		return Message(self.tag, queries_id, queries_type, self.payload)

	# For subclasses
	def ack_self(self):
		pass

	def describe(self):
		logging.info(f"action: msg_describe | result: success | tag: {self.tag} | queries_id:{self.ids} | queries_type: {self.types}")

	def stream_rows(self):
		logging.info(f"action: stream_rows | result: success | data: {self.payload}")
		return []

	def is_partition_eof(self):
		return self.payload == None

	def is_eof(self):
		return self.partition < 0 # Negative partition es eof, be it an error or actual eof.