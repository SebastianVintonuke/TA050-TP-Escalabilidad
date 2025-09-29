import logging

from common.config.base_type_config import (
    BaseDictTypeConfiguration,
    BaseTypeConfiguration,
)
from common.config.row_filtering import load_all_filters, should_keep


JOIN_ACTION_ID = 0
JOIN_CONDITIONS = 1

class JoinTypeConfiguration:

    def __init__(
        self, out_middleware, builder_creator, 
        left_type,in_fields_left,in_fields_right, 
        join_conf, out_mapper
    ):
        self.joiner = load_joiner(join_conf[JOIN_CONDITIONS])
        self.join_id = join_conf[JOIN_ACTION_ID]
        self.left_type = left_type
        self.out_mapper = out_mapper

    def do_join_left_row(self, right_rows, left_row, join_receiver):
        for right_row in right_rows:
            if self.joiner.should_join(left_row, right_row):
                join_receiver(self.out_mapper(left_row, right_row))
                #return # If join is only once per row then this

    def do_join_right_row(self, left_rows, right_row, join_receiver):
        for left_row in left_rows:
            if self.joiner.should_join(left_row, right_row):
                join_receiver(self.out_mapper(left_row, right_row))

    def filter_map(self, row):
        try:
            # No pad needed ? self.pad_copy_row(row), mapping to dict and if not enough rows... fail
            row = self.mapper.map_input(row)
            # logging.info(f"MAPPED ROW {row}")
            if self.should_keep(row):
                return self.mapper(row)
            return None
        except Exception as e:
            logging.error(f"Failed filter map of row {row} invalid {e}")
            return None
