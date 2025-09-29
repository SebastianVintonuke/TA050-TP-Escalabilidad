import unittest

from middleware.mocks.middleware import *
from common.config.row_joining import *
from joinnode.src.join_type_config import *
from joinnode.src.join_accumulator import *
from joinnode.src.joinnode import *
from common.config.type_expander import *


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

class TestJoinAccumulator(unittest.TestCase):
    def test_query_2_revenue_for_join_does_not_send_if_not_eof(self):
        result_grouper = MockCopyMiddleware()
        in_left = ["product_id","product_name"]
        in_right = ["top_product_id","month","revenue",]
        out_cols = ["product_id","product_name","month","revenue",]

        config = JoinTypeConfiguration(result_grouper, BareMockMessageBuilder,
            left_type= "LEFT", #
            in_fields_left=in_left,  # ..product names
            in_fields_right=in_right,
            join_id = "join_id",
            join_conf=[INNER_ON_EQ, {"col_left":"product_id", "col_right":"top_product_id"}],
            out_cols= out_cols
        )
        
        rows_left = [
            {"product_name":"PRODUCT NAME 1", "product_id": "prod_1"},
            {"product_name":"PRODUCT NAME 2", "product_id": "prod_2"},
            {"product_name":"PRODUCT NAME 3", "product_id": "prod_3"},
        ]

        rows_right = [
            {"top_product_id": "prod_1", "month": 1, "revenue": 10},
            {"top_product_id": "prod_1", "month": 2, "revenue": 20},
            {"top_product_id": "prod_2", "month": 3, "revenue": 30},
            {"top_product_id": "prod_4", "month": 4, "revenue": 40},
        ]

        rows_left = [map_dict_to_vect_cols(in_left, row) for row in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, row) for row in rows_right]

        expected_out = [
            {"product_id": "prod_1", "product_name": "PRODUCT NAME 1", "month": 1, "revenue": 10},
            {"product_id": "prod_1", "product_name": "PRODUCT NAME 1", "month": 2, "revenue": 20},
            {"product_id": "prod_2", "product_name": "PRODUCT NAME 2", "month": 3, "revenue": 30},
        ]
        expected_out = [map_dict_to_vect_cols(out_cols, row) for row in expected_out]

        types_expander = TypeExpander()
        in_middle = MockMiddleware()

        types_expander.add_configuration_to_many(config, "LEFT", "RIGHT")

        node = JoinNode(in_middle, types_expander)
        node.start()

        message = MockMessage(
            "tag1",
            ["user_id"],
            ["LEFT"],
            rows_left,
            lambda r: r,
        )
        in_middle.push_msg(message)
        message = MockMessage(
            "tag2",
            ["user_id"],
            ["RIGHT"],
            rows_right,
            lambda r: r,
        )
        in_middle.push_msg(message)

        self.assertEqual(len(result_grouper.msgs), 0)

    def test_query_2_revenue(self):
        result_grouper = MockCopyMiddleware()
        in_left = ["product_id","product_name"]
        in_right = ["top_product_id","month","revenue",]
        out_cols = ["product_id","product_name","month","revenue",]

        config = JoinTypeConfiguration(result_grouper, BareMockMessageBuilder,
            left_type= "LEFT", #
            in_fields_left=in_left,  # ..product names
            in_fields_right=in_right,
            join_id = "join_id",
            join_conf=[INNER_ON_EQ, {"col_left":"product_id", "col_right":"top_product_id"}],
            out_cols= out_cols
        )
        
        rows_left = [
            {"product_name":"PRODUCT NAME 1", "product_id": "prod_1"},
            {"product_name":"PRODUCT NAME 2", "product_id": "prod_2"},
            {"product_name":"PRODUCT NAME 3", "product_id": "prod_3"},
        ]

        rows_right = [
            {"top_product_id": "prod_1", "month": 1, "revenue": 10},
            {"top_product_id": "prod_1", "month": 2, "revenue": 20},
            {"top_product_id": "prod_2", "month": 3, "revenue": 30},
            {"top_product_id": "prod_4", "month": 4, "revenue": 40},
        ]

        rows_left = [map_dict_to_vect_cols(in_left, row) for row in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, row) for row in rows_right]

        expected_out = [
            {"product_id": "prod_1", "product_name": "PRODUCT NAME 1", "month": 1, "revenue": 10},
            {"product_id": "prod_1", "product_name": "PRODUCT NAME 1", "month": 2, "revenue": 20},
            {"product_id": "prod_2", "product_name": "PRODUCT NAME 2", "month": 3, "revenue": 30},
        ]
        expected_out = [map_dict_to_vect_cols(out_cols, row) for row in expected_out]

        types_expander = TypeExpander()
        in_middle = MockMiddleware()

        types_expander.add_configuration_to_many(config, "LEFT", "RIGHT")

        node = JoinNode(in_middle, types_expander)
        node.start()

        message = MockMessage(
            "tag1",
            ["user_id"],
            ["LEFT"],
            rows_left,
            lambda r: r,
        )
        in_middle.push_msg(message)
        message = MockMessage(
            "tag2",
            ["user_id"],
            ["RIGHT"],
            rows_right,
            lambda r: r,
        )
        in_middle.push_msg(message)

        # EOFS LEFT
        eof_message = MockMessage("tag1",["user_id"],["LEFT"],[], lambda r: r) 
        eof_message.set_partition_eof()
        in_middle.push_msg(eof_message)
        self.assertEqual(len(result_grouper.msgs), 0)

        #FINAL EOF
        eof_message = MockMessage("tag1",["user_id"],["LEFT"],[], lambda r: r) 
        eof_message.set_eof()
        in_middle.push_msg(eof_message)        

        # LIMITED BY LIMIT 
        self.assertEqual(len(result_grouper.msgs), 0)

        # EOFS RIGHT
        eof_message = MockMessage("tag1",["user_id"],["RIGHT"],[], lambda r: r) 
        eof_message.set_partition_eof()
        in_middle.push_msg(eof_message)
        self.assertEqual(len(result_grouper.msgs), 0)

        #FINAL EOF
        eof_message = MockMessage("tag1",["user_id"],["RIGHT"],[], lambda r: r) 
        eof_message.set_eof()
        in_middle.push_msg(eof_message)        

        self.assertEqual(len(result_grouper.msgs), 3)
        self.assertEqual(result_grouper.msgs[-1].is_finish(), True)
        self.assertEqual(result_grouper.msgs[-2].is_eof(), True)
        
        rows_out = result_grouper.msgs[0].payload

        self.assertEqual(len(rows_out), len(expected_out))
        for i in range(len(expected_out)):
            self.assertEqual(rows_out[i], expected_out[i])


        #self.assertEqual(.is_eof(), True)


