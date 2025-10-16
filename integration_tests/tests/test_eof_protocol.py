import unittest

from middleware.mocks.middleware import *

from common.config.row_filtering import *
from common.config.row_mapping import *
from common.config.type_expander import *
from selectnode.src.select_type_config import *
from selectnode.src.selectnode import *
from selectnode.src.config_init import *


def map_dict_to_vect_cols(cols, row):
    res = []
    for col in cols:
        res.append(str(row[col]))
    return res


def map_vect_to_dict_cols(cols, row):
    res = {}
    for i in range(len(cols)):
        res[cols[i]] = row[i]
    return res


class TestEOFProtocol(unittest.TestCase):
    def test_compiled_config(self):
        types_expander = TypeExpander()
        result_grouper = MockMiddleware()
        groupby_middleware = MockMiddleware()
        add_selectnode_config(types_expander, result_grouper, groupby_middleware)
        self.assertFalse(False) # Just be able to compile it basically.

    def test_query_1_selectnode(self):

        # In order
        in_cols = ["transaction_id", "year", "hour", "sum"]
        out_cols = ["transaction_id", "sum"]

        result_grouper = MockMiddleware()
        type_conf = SelectTypeConfiguration(
            result_grouper,
            BareMockMessageBuilder,
            in_fields=in_cols,
            filters_conf=[
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
            ],
            out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )

        in_middle = MockMiddleware()

        type_exp = TypeExpander()
        type_exp.add_configurations("t1", type_conf)

        node = SelectNode(in_middle, MockMessage, type_exp)
        node.start_single()

        rows_pass = [
            {"transaction_id": "tr1", "year": 2024, "hour": 7, "sum": 88},
            {"transaction_id": "tr2", "year": 2025, "hour": 23, "sum": 942},
            {"transaction_id": "tr3", "year": 2024, "hour": 6, "sum": 942},
        ]
        # out_cols = ["transaction_id", "sum"]
        expected = [[r["transaction_id"], str(r["sum"])] for r in rows_pass]

        rows_fail = [
            {"transaction_id": "tr4", "year": 2027, "hour": 7, "sum": 88},
            {"transaction_id": "tr5", "year": 2025, "hour": 24, "sum": 942},
            {"transaction_id": "tr6", "year": 2024, "hour": 6, "sum": 55},
        ]

        message = BareMockMessageBuilder.for_payload(
            ["query_3323"],
            ["t1"],
            rows_pass + rows_fail,
            lambda r: map_dict_to_vect_cols(in_cols, r),
        )

        in_middle.push_msg(message)

        self.assertEqual(len(result_grouper.msgs), message.headers.len_queries())

        for ind, exp_out_headers in enumerate(message.headers.split()):            
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_out_headers.to_dict())

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result), len(rows_pass))

        ind = 0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind += 1
