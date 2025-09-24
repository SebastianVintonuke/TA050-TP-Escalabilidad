import logging
from .header_fields import *

# Message builder
class MessageBuilder:
	def __init__(self,queries_id, queries_type):
		self.ids = queries_id
		self.types = queries_type
		self.payload = []
		self.fields= []

	def set_fields(self, fields):
		# set field names!
		self.fields = fields

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


# Defines basic initialization and header management
class Message:
	def _verify_headers(self):
		# assert len of queries == len of types!
		assert len(self.ids) == len(self.types)

	def _from_headers(self, headers):
		self.ids = headers.get(FIELD_QUERY_ID, [])
		self.types = headers.get(FIELD_QUERY_TYPE, [])

		self._verify_headers()

	def _deserialize_payload(self, payload):
		return payload

	def from_data(self, tag, queries_id, queries_type, payload):
		self.tag = tag
		self.ids = queries_id
		self.types = queries_type
		self._verify_headers()
		self.payload = payload

	def __init__(self, tag, headers, payload):
		self.tag = tag
		self._from_headers(headers)
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