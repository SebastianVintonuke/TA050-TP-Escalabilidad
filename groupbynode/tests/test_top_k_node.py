import unittest

from middleware.mocks.middleware import *
from common.config.row_mapping import *
from common.config.type_expander import *

from groupbynode.src.row_aggregate import *
from groupbynode.src.row_grouping import *

from groupbynode.src.topk_type_config import *
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


class TestTopKNode(unittest.TestCase):

    def test_query_2_topknode(self):

        # In order
        in_cols = ["product_id", "month", "revenue", "quantity_sold"]
        out_cols = ["product_id", "month", "revenue"]

        result_grouper = MockMiddleware()
        type_conf = TopKTypeConfiguration(result_grouper, MockMessageBuilder, 
                in_fields = in_cols, #EQUALS to out cols from select node main 
                grouping_conf = [
                    ["month"], [KEEP_TOP_K, {'comp_key': "revenue", 'limit': 3}]
                ],
                out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )


        in_middle = MockMiddleware()

        type_exp= {
            "t1": type_conf
        }
        node = GroupbyNode(in_middle, type_exp)
        node.start()

        rows = [
            {"product_id": "pr1", "month": 7, "revenue": 176, "quantity_sold": 2},
            {"product_id": "pr1", "month": 8, "revenue": 30, "quantity_sold": 2},
            
            {"product_id": "pr2", "month": 7, "revenue": 942, "quantity_sold": 1},
            {"product_id": "pr4", "month": 7, "revenue": 942, "quantity_sold": 1},
            
            {"product_id": "pr3", "month": 7, "revenue": 942, "quantity_sold": 2},
        ]
        expected = [
            ["pr2", "7", "942"],
            ["pr4", "7", "942"],
            ["pr3", "7", "942"],
            ["pr1", "8", "30"],
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

        ind = 0
        for elem in expected:
            self.assertIn(got_result[ind], got_result)
            ind += 1

        self.assertEqual(len(got_result), len(expected))



    def test_query_2_topknode_quantity_wise(self):

        # In order
        in_cols = ["product_id", "month", "revenue", "quantity_sold"]
        out_cols = ["product_id", "month", "quantity_sold"]

        result_grouper = MockMiddleware()
        type_conf = TopKTypeConfiguration(result_grouper, MockMessageBuilder, 
                in_fields = in_cols, #EQUALS to out cols from select node main 
                grouping_conf = [
                    ["month"], [KEEP_TOP_K, {'comp_key': "quantity_sold", 'limit': 3}]
                ],
                out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )


        in_middle = MockMiddleware()

        type_exp= {
            "t1": type_conf
        }
        node = GroupbyNode(in_middle, type_exp)
        node.start()

        rows = [
            {"product_id": "pr1", "month": 7, "revenue": 176, "quantity_sold": 2},
            {"product_id": "pr1", "month": 8, "revenue": 30, "quantity_sold": 2},
            
            {"product_id": "pr2", "month": 7, "revenue": 942, "quantity_sold": 1},
            {"product_id": "pr4", "month": 7, "revenue": 942, "quantity_sold": 1},
            
            {"product_id": "pr3", "month": 7, "revenue": 942, "quantity_sold": 2},
        ]
        expected = [
            ["pr1", "7", "2"],
            ["pr3", "7", "2"],
            ["pr2", "7", "1"],
            ["pr1", "8", "2"],
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

        ind = 0
        for elem in expected:
            self.assertIn(got_result[ind], got_result)
            ind += 1

        self.assertEqual(len(got_result), len(expected))
