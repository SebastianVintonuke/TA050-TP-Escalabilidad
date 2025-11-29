FIELD_QUERY_ID ="queries_id" 
FIELD_QUERY_TYPE ="queries_type" 
FIELD_PARTITION_IND ="partition_ind"
FIELD_PACKET_ID ="packet_id"

DEFAULT_PARTITION_VALUE =-1
DEFAULT_QUERY_TYPE =""

EOF_SIGNAL = -1
DEFAULT_ERROR_SIGNAL= -2
UNDEFINED_PACKET_ID= -1
#FIELD_QUERY_FIELDS ="queries_" 

class BaseHeaders:

	def default():
		return BaseHeaders([],[])

	def from_headers(headers):
		return BaseHeaders(
			headers.get(FIELD_QUERY_ID, []),
			headers.get(FIELD_QUERY_TYPE, [DEFAULT_QUERY_TYPE]),
			headers.get(FIELD_PARTITION_IND, DEFAULT_PARTITION_VALUE),
			headers.get(FIELD_PACKET_ID, UNDEFINED_PACKET_ID)
		)

	def from_headers_typed(headers, q_type):
		return BaseHeaders(
			[headers.get(FIELD_QUERY_ID, None)],
			[q_type],
			headers.get(FIELD_PARTITION_IND, DEFAULT_PARTITION_VALUE),
			headers.get(FIELD_PACKET_ID, UNDEFINED_PACKET_ID)			
		)

	def __init__(self, ids, types = [DEFAULT_QUERY_TYPE], msg_count = DEFAULT_PARTITION_VALUE, packet_id = UNDEFINED_PACKET_ID):
		self.ids = ids
		self.types = types
		self.msg_count = msg_count
		self.packet_id = packet_id

	def __repr__(self):
		return f"ids:{self.ids}, types:{self.types}, msg_count:{self.msg_count} pck_id: {self.packet_id}"

	def with_types_pad(self, types):
		ids = list(self.ids) + (len(types) - len(self.ids)) * [self.ids[-1]]
		
		return BaseHeaders(
			ids,
			types,
			self.msg_count,
			self.packet_id
		)


	def with_types(self, types):
		return BaseHeaders(
			list(self.ids),
			types,
			self.msg_count,
			self.packet_id
		)
	def clone(self):
		return BaseHeaders(
			list(self.ids),
			list(self.types),
			self.msg_count,
			self.packet_id
		)

	def get_error_code(self):
		return self.msg_count

	def len_queries(self):
		return len(self.ids)
	def describe(self):
		pass
		#logging.info(f"action: msg_describe | result: success | queries_id:{self.ids} | queries_type: {self.types}")

	def reset_eof(self):
		self.msg_count = DEFAULT_PARTITION_VALUE

	def is_eof(self):
		return self.msg_count != DEFAULT_PARTITION_VALUE

	def is_error(self):
		return self.msg_count < 0 and self.msg_count != DEFAULT_PARTITION_VALUE # Negative partition means error

	def iter_credentials(self):
		return zip(self.ids, self.types)		

	def sub_for(self, ind):
		return BaseHeaders([self.ids[ind]], [self.types[ind]], self.msg_count, self.packet_id)
	
	def first_query(self):
		return BaseHeaders([self.ids[0]],[self.types[0]],self.msg_count, self.packet_id)

	def split(self):
		for ide, type in zip(self.ids, self.types):
			yield BaseHeaders([ide],[type],self.msg_count, self.packet_id)

	def iter_type_headers(self):
		if self.msg_count != DEFAULT_PARTITION_VALUE: #If eof add it, else ignore the header.		
			for ide, type in zip(self.ids, self.types):
				yield (type,{
					FIELD_QUERY_ID: ide,
					FIELD_PARTITION_IND: self.msg_count
				})
		else:
			for ide, type in zip(self.ids, self.types):
				yield (type,{
					FIELD_QUERY_ID: ide,
				})


	def to_dict(self):
		res = {
			FIELD_QUERY_ID: self.ids,
			FIELD_PACKET_ID: self.packet_id
		}

		if len(self.types) >0 and self.types[0] != DEFAULT_QUERY_TYPE:
			res[FIELD_QUERY_TYPE] = self.types

		if self.msg_count != DEFAULT_PARTITION_VALUE: # If it is the default one, then save it.
			res[FIELD_PARTITION_IND]= self.msg_count

		return res

	def to_dict_no_type(self):
		res = {
			FIELD_QUERY_ID: self.ids,
		}
		
		if self.msg_count != DEFAULT_PARTITION_VALUE: # If it is the default one, then save it.
			res[FIELD_PARTITION_IND]= self.msg_count

		return res