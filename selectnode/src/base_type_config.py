
from .row_mapping import DictConvertWrapperMapper, RowMapper, NoActionRowMapper,ROW_CONFIG_OUT_COLS
import logging


class BaseTypeConfiguration:

	def __init__(self, out_middleware, builder_creator, in_fields_count):
		self.middleware = out_middleware
		self.builder_creator = builder_creator
		self.in_fields_count = in_fields_count # ["year","month","some"] == 3 fields

		self.mapper = NoActionRowMapper()

	def load_mapper(self, config):
		self.mapper= RowMapper(config)

	def new_builder_for(self, inp_msg, ind_query):
		return self.builder_creator(inp_msg, ind_query)

	def send(self, builder):
		return self.middleware.send(builder)
	
	def filter_map_row(self, row):
		try:
			return self.mapper(row)
		except Exception as e:
			logging.error(f"Failed filter map of row {row} invalid {e}")
			return None

class BaseDictTypeConfiguration(BaseTypeConfiguration):

	def __init__(self, out_middleware, builder_creator, in_fields, out_conf = None):
		super().__init__(out_middleware, builder_creator, len(in_fields))
		if out_conf != None:
			self.load_mapper(out_conf)
			self.mapper	= DictConvertWrapperMapper(in_fields, self.mapper, out_conf[ROW_CONFIG_OUT_COLS])		
		else:
			self.mapper	= DictConvertWrapperMapper(in_fields, self.mapper, in_fields)		


	def _map_input_row(self, row):
		return row

	# By default it doesnt do anything, row its already a vector with its values
	# But If converted to something it should get the vector of values from row
	def _default_map_output(self, row):
		return row



