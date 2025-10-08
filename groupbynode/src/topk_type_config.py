
from common.config.row_mapping import DictConvertWrapperMapper,NoActionRowMapper, ROW_CONFIG_OUT_COLS

from .row_grouping import RowGrouper
from .row_key_parsing import *
import logging

TOPK_KEY_FIELDS = 0
TOPK_FIELDS_ACTIONS = 1

class TopKTypeConfiguration:
	def __init__(self, out_middleware, builder_creator, in_fields, grouping_conf, out_conf=None):
		self.middleware = out_middleware
		self.new_builder_for = builder_creator

		self.mapper = DictConvertWrapperMapper(
			in_fields, NoActionRowMapper(), out_conf[ROW_CONFIG_OUT_COLS]
		)

		self.key_parser = KeyGroupParser(grouping_conf[TOPK_KEY_FIELDS])
		self.grouper = RowGrouper(grouping_conf[TOPK_FIELDS_ACTIONS])

	def map_input_row(self, row):
		return self.mapper.map_input(row)

	def add_output(self, msg_builder, group_key, acc):
		for row in self.grouper.iterate_rows(acc):
			msg_builder.add_row(self.mapper.project_out(row))

	def send(self, builder):
		logging.info(f"TOPK SENDING TO {builder.headers.types} {builder.headers.ids} len: {builder.len_payload()} eof? {builder.headers.is_eof()}")
		#for itm in builder.payload:
		#	logging.info(f"ROW {itm}")
			
		return self.middleware.send(builder)