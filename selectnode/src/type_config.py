
from .row_filtering import load_all_filters, should_keep
from .row_mapping import map_dict_to_vec, RowMapper
import logging




class TypeConfiguration:

	def __init__(self, out_middleware, builder_creator, in_fields, filters_conf, out_conf = None):
		self.middleware = out_middleware
		self.builder_creator = builder_creator
		self.in_fields = in_fields # For now map to dict

		self.filters = load_all_filters(filters_conf)
		self.map_to_output = lambda row: map_dict_to_vec(self.in_fields, row)

		if out_conf != None:
			self.map_to_output= RowMapper(out_conf)

	def should_keep(self, row):
		return should_keep(self.filters, row)

	def new_builder_for(self, inp_msg, ind_query):
		return self.builder_creator(inp_msg, ind_query)

	def send(self, builder):
		return self.middleware.send(builder)
	
	# Map to dict by default?
	def _map_input_row(self, row):
		res={}
		for ind , col in enumerate(self.in_fields):
			res[col] = row[ind]
		return res


	def filter_map_row(self, row):
		try:
			row = self._map_input_row(row)
			
			if self.should_keep(row):
				return self.map_to_output(row)

		except Exception as e:
			logging.error(f"Failed filter map of row {row} invalid {e}")
			return None
