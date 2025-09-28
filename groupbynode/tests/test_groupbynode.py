import unittest

from middleware.mocks.middleware import *
from common.config.row_mapping import *
from common.config.type_expander import *

from groupbynode.src.row_grouping import *

from groupbynode.src.groupby_type_config import *
from groupbynode.src.groupbynode import *


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


class TestSelectNode(unittest.TestCase):

    def test_groupbynode_does_not_send_result_if_not_eof(self):

        # In order
        in_cols = ["product_id", "month", "revenue"]
        out_cols = ["product_id", "month", "revenue", "quantity_sold"]

        result_grouper = MockMiddleware()
        type_conf = GroupbyTypeConfiguration(result_grouper, MockMessageBuilder, 
                in_fields = in_cols, #EQUALS to out cols from select node main 
                grouping_conf = [["product_id", "month"], {
                    "revenue": SUM_ACTION,
                    "quantity_sold": COUNT_ACTION
                }],
                out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )


        in_middle = MockMiddleware()

        type_exp= {
            "t1": type_conf
        }
        node = GroupbyNode(in_middle, type_exp)
        node.start()

        rows = [
            {"product_id": "pr1", "month": 7, "revenue": 88},
            {"product_id": "pr1", "month": 7, "revenue": 88},
            {"product_id": "pr1", "month": 8, "revenue": 10},
            
            {"product_id": "pr2", "month": 7, "revenue": 942},
            {"product_id": "pr2", "month": 23, "revenue": 942},
            
            {"product_id": "pr3", "month": 6, "revenue": 942},
        ]
        expected = [
            ["pr1", "7", "196", "2"],
            ["pr1", "8", "10", "1"],
            ["pr2", "23", "942", "1"],
            ["pr2", "7", "942", "1"],
            ["pr3", "6", "942", "1"],
        ]

        message = MockMessage(
            "tag1",
            ["query_3323"],
            ["t1"],
            rows,
            lambda r: map_dict_to_vect_cols(in_cols, r),
        )

        in_middle.push_msg(message)

        self.assertEqual(len(result_grouper.msgs), 0)

    def test_query_2_groupbynode(self):

        # In order
        in_cols = ["product_id", "month", "revenue"]
        out_cols = ["product_id", "month", "revenue", "quantity_sold"]

        result_grouper = MockMiddleware()
        type_conf = GroupbyTypeConfiguration(result_grouper, MockMessageBuilder, 
                in_fields = in_cols, #EQUALS to out cols from select node main 
                grouping_conf = [["product_id", "month"], {
                    "revenue": SUM_ACTION,
                    "quantity_sold": COUNT_ACTION
                }],
                out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )


        in_middle = MockMiddleware()

        type_exp= {
            "t1": type_conf
        }
        node = GroupbyNode(in_middle, type_exp)
        node.start()

        rows = [
            {"product_id": "pr1", "month": 7, "revenue": 88},
            {"product_id": "pr1", "month": 7, "revenue": 88},
            {"product_id": "pr1", "month": 8, "revenue": 10},
            
            {"product_id": "pr2", "month": 7, "revenue": 942},
            {"product_id": "pr2", "month": 23, "revenue": 942},
            
            {"product_id": "pr3", "month": 6, "revenue": 942},
        ]
        expected = [
            ["pr1", "7", "176.0", "2"],
            ["pr1", "8", "10.0", "1"],
            ["pr2", "7", "942.0", "1"],
            ["pr2", "23", "942.0", "1"],
            ["pr3", "6", "942.0", "1"],
        ]
        map_f = lambda r: map_dict_to_vect_cols(in_cols, r)
        message = MockMessage(
            "tag1",
            ["query_3323"],
            ["t1"],
            rows,map_f,
        )

        in_middle.push_msg(message)

        #Partition eof
        eof_message = MockMessage("tag1",["query_3323"],["t1"],[], map_f) 
        eof_message.set_partition_eof()
        in_middle.push_msg(eof_message)
        self.assertEqual(len(result_grouper.msgs), 0)

        #FINAL EOF
        eof_message = MockMessage("tag1",["query_3323"],["t1"],[], map_f) 
        eof_message.set_eof()
        in_middle.push_msg(eof_message)
        self.assertEqual(len(result_grouper.msgs), 3)
        self.assertEqual(result_grouper.msgs[0].ind, 0)
        #self.assertEqual(result_grouper.msgs[0].msg_from, message)

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result), len(expected))
        ind = 0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind += 1