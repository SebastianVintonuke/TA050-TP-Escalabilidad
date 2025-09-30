import logging

from common.config.base_type_config import (
    BaseDictTypeConfiguration,
    BaseTypeConfiguration,
)
from common.config.row_joining import *


JOIN_ACTION_ID = 0
JOIN_CONDITIONS = 1

def divide_out_cols(in_left, in_right, out_cols):
    out_left = []
    out_right = []

    for col in out_cols:
        if col in in_left:
            out_left.append(col)
        else:
            assert col in in_right
            out_right.append(col)

    return (out_left, out_right)


def mapped_divide_out_cols(in_left, in_right, out_cols):
    out_left = []
    out_right = []

    ind = 0
    for col in out_cols:
        try:
            out_left.append([ind, in_left.index(col)])
        except ValueError: # python bs... it means not in out left. better than a for since its cpython.
            out_right.append([ind, in_right.index(col)])
        ind += 1

    return (out_left, out_right)


class JoinTypeConfiguration:

    def __init__(
        self, out_middleware, builder_creator, 
        left_type,in_fields_left,in_fields_right, 
        join_id,join_conf, out_cols = None
    ):
        self.joiner = load_joiner(join_conf)

        self.joiner.map_cols(
            lambda col_left: in_fields_left.index(col_left),
            lambda col_right: in_fields_right.index(col_right),
        )

        self.join_id = join_id
        self.left_type = left_type


        if out_cols == None:
            self.out_mapper = JoinProjectMapperAll()
        else:
            out_left, out_right = mapped_divide_out_cols(in_fields_left, in_fields_right, out_cols)

            self.out_mapper = JoinProjectMapperOrdered(out_left, out_right)

        self.builder_creator = builder_creator
        self.middleware = out_middleware

    def new_builder_for(self, inp_msg, ind_query):
        return self.builder_creator(inp_msg, ind_query)
    def send(self, builder):
        return self.middleware.send(builder)

    def do_join_left_row(self, right_rows, left_row, join_receiver):
        for right_row in right_rows:
            if self.joiner.should_join(left_row, right_row):
                join_receiver(self.out_mapper(left_row, right_row))
                #return # If join is only once per row then this

    def do_join_right_row(self, left_rows, right_row, join_receiver):
        for left_row in left_rows:
            if self.joiner.should_join(left_row, right_row):
                join_receiver(self.out_mapper(left_row, right_row))