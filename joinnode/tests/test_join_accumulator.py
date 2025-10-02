import unittest

from middleware.mocks.middleware import *
from common.config.row_joining import *
from joinnode.src.join_type_config import *
from joinnode.src.join_accumulator import *



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
    def build_simple_join_config(self,  result_grouper):
        in_left = ["product_id","product_name"]
        in_right = ["top_product_id","month","revenue",]
        out_cols = ["product_id","product_name","month","revenue",]

        config_type = JoinTypeConfiguration(result_grouper, BareMockMessageBuilder,
            left_type= "LEFT", #
            in_fields_left=in_left,  # ..product names
            in_fields_right=in_right,
            join_id = "join_id",
            join_conf=[INNER_ON_EQ, {"col_left":"product_id", "col_right":"top_product_id"}],
            out_cols= out_cols
        )

        return (in_left, in_right, out_cols, config_type)


    def test_simple_bordercase_mixed_out_column_order(self):
        in_left = ["product_id","product_name"]
        in_right = ["top_product_id","month","revenue",]
        out_cols = ["month", "product_id","product_name","revenue",]
        result_grouper = MockCopyMiddleware()

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
        ]

        rows_right = [
            {"top_product_id": "prod_1", "month": 2, "revenue": 20},
        ]
        rows_left = [map_dict_to_vect_cols(in_left, row) for row in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, row) for row in rows_right]

        expected_out = [
            {"product_id": "prod_1", "product_name": "PRODUCT NAME 1", "month": 2, "revenue": 20},
        ]
        expected_out = [map_dict_to_vect_cols(out_cols, row) for row in expected_out]


        out_right = []
        for row_right in rows_right:
            config.do_join_right_row(rows_left, row_right, out_right.append)

        self.assertEqual(len(out_right), len(expected_out))
        for i in range(len(expected_out)):
            self.assertEqual(out_right[i], expected_out[i])


    def test_simple_bordercase_same_name_col(self):
        in_left = ["product_id","product_name"]
        in_right = ["product_id","month","revenue",]
        out_cols = ["month", "product_id","product_name","revenue",]
        result_grouper = MockCopyMiddleware()

        config = JoinTypeConfiguration(result_grouper, BareMockMessageBuilder,
            left_type= "LEFT", #
            in_fields_left=in_left,  # ..product names
            in_fields_right=in_right,
            join_id = "join_id",
            join_conf=[INNER_ON_EQ, {"col_left":"product_id", "col_right":"product_id"}],
            out_cols= out_cols
        )

        rows_left = [
            {"product_name":"PRODUCT NAME 1", "product_id": "prod_1"},
        ]

        rows_right = [
            {"product_id": "prod_1", "month": 2, "revenue": 20},
        ]
        rows_left = [map_dict_to_vect_cols(in_left, row) for row in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, row) for row in rows_right]

        expected_out = [
            {"product_id": "prod_1", "product_name": "PRODUCT NAME 1", "month": 2, "revenue": 20},
        ]
        expected_out = [map_dict_to_vect_cols(out_cols, row) for row in expected_out]


        out_right = []
        for row_right in rows_right:
            config.do_join_right_row(rows_left, row_right, out_right.append)

        self.assertEqual(len(out_right), len(expected_out))
        for i in range(len(expected_out)):
            self.assertEqual(out_right[i], expected_out[i])




    def test_simple_join_type_config(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)
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

        out_right = []
        for row_right in rows_right:
            config.do_join_right_row(rows_left, row_right, out_right.append)
        self.assertEqual(len(out_right), len(expected_out))
        for i in range(len(expected_out)):
            self.assertEqual(out_right[i], expected_out[i])

        out_left = []
        for row_left in rows_left:
            config.do_join_left_row(rows_right, row_left, out_left.append)

        self.assertEqual(len(out_left), len(expected_out))
        for i in range(len(expected_out)):
            self.assertEqual(out_left[i], expected_out[i])


    def test_simple_join_accumulator_does_not_send_anything_if_not_eof(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)
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

        accumulator = JoinAccumulator(config, None, 0, limit = 1) # No need to mark a message nor index

        left_action = accumulator.get_action_for_type("LEFT")

        for left_row in rows_left:
            left_action(left_row)

        right_action = accumulator.get_action_for_type("RIGHT")
        for right_row in rows_right:
            right_action(right_row)

        self.assertEqual(len(result_grouper.msgs), 0)

    def test_simple_join_accumulator_does_not_send_anything_if_not_eof(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)
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

        accumulator = JoinAccumulator(config, None, 0, limit = 1) # No need to mark a message nor index

        left_action = accumulator.get_action_for_type("LEFT")

        for left_row in rows_left:
            left_action(left_row)

        right_action = accumulator.get_action_for_type("RIGHT")
        for right_row in rows_right:
            right_action(right_row)

        self.assertEqual(len(result_grouper.msgs), 0)

    def test_inner_join_with_matching_rows(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)

        rows_left = [
            {"product_name":"A", "product_id": "id1"},
        ]
        rows_right = [
            {"top_product_id": "id1", "month": 1, "revenue": 100},
        ]

        expected_output = [
            {"product_id": "id1", "product_name": "A", "month": 1, "revenue": 100}
        ]

        rows_left = [map_dict_to_vect_cols(in_left, r) for r in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, r) for r in rows_right]
        expected_output = [map_dict_to_vect_cols(out_cols, r) for r in expected_output]

        acc = JoinAccumulator(config, None, 0, limit=10000)
        left_action = acc.get_action_for_type("LEFT")
        right_action = acc.get_action_for_type("RIGHT")

        for left_row in rows_left:
            left_action(left_row)
        for right_row in rows_right:
            right_action(right_row)

        acc.handle_eof_left()
        acc.handle_eof_right()

        self.assertEqual(len(result_grouper.msgs), 2) # payload msg and 1 for eof.
        output_payload = result_grouper.msgs[0].payload
        self.assertEqual(output_payload, expected_output)

    def test_inner_join_with_no_matches(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)

        rows_left = [
            {"product_name":"A", "product_id": "id1"},
        ]
        rows_right = [
            {"top_product_id": "idX", "month": 1, "revenue": 100},
        ]

        rows_left = [map_dict_to_vect_cols(in_left, r) for r in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, r) for r in rows_right]

        acc = JoinAccumulator(config, None, 0)
        left_action = acc.get_action_for_type("LEFT")
        right_action = acc.get_action_for_type("RIGHT")

        for left_row in rows_left:
            left_action(left_row)
        for right_row in rows_right:
            right_action(right_row)

        acc.handle_eof_left()
        acc.handle_eof_right()

        # Nothing should have been sent
        self.assertEqual(len(result_grouper.msgs), 1)  # Only EOF
        self.assertEqual(result_grouper.msgs[-1].is_eof(), True)

    def test_join_flushes_when_limit_hit(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)

        rows_left = [{"product_name": "A", "product_id": "id1"}]
        rows_right = [{"top_product_id": "id1", "month": i, "revenue": i * 10} for i in range(5)]

        rows_left = [map_dict_to_vect_cols(in_left, r) for r in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, r) for r in rows_right]

        acc = JoinAccumulator(config, None, 0, limit=1)  # low limit to force flushing

        left_action = acc.get_action_for_type("LEFT")
        right_action = acc.get_action_for_type("RIGHT")

        for left_row in rows_left:
            left_action(left_row)
        for right_row in rows_right:
            right_action(right_row)

        acc.handle_eof_left()
        acc.handle_eof_right()

        # Each join should flush immediately due to limit
        num_expected_rows = 5
        flushes = [msg for msg in result_grouper.msgs if not msg.is_eof()]
        self.assertEqual(len(flushes), num_expected_rows)

    def test_eof_triggers_output_send(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)

        rows_left = [{"product_name": "A", "product_id": "id1"}]
        rows_right = [{"top_product_id": "id1", "month": 1, "revenue": 10}]

        rows_left = [map_dict_to_vect_cols(in_left, r) for r in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, r) for r in rows_right]

        acc = JoinAccumulator(config, None, 0, limit=10000)

        acc.get_action_for_type("LEFT")(rows_left[0])
        acc.get_action_for_type("RIGHT")(rows_right[0])

        acc.handle_eof_left()
        acc.handle_eof_right()

        # Should emit the payload and one EOF message
        self.assertEqual(len(result_grouper.msgs), 2)
        self.assertTrue(result_grouper.msgs[-1].is_eof())

    def test_right_finishes_before_left(self):
        result_grouper = MockCopyMiddleware()
        in_left, in_right, out_cols, config = self.build_simple_join_config(result_grouper)

        rows_left = [{"product_name": "A", "product_id": "id1"}]
        rows_right = [{"top_product_id": "id1", "month": 1, "revenue": 10}]

        rows_left = [map_dict_to_vect_cols(in_left, r) for r in rows_left]
        rows_right = [map_dict_to_vect_cols(in_right, r) for r in rows_right]

        acc = JoinAccumulator(config, None, 0)
        acc.get_action_for_type("RIGHT")(rows_right[0])
        acc.handle_eof_right()
        acc.get_action_for_type("LEFT")(rows_left[0])
        acc.handle_eof_left()

        self.assertEqual(len(result_grouper.msgs), 2)  # One data msg + EOF
        payloads = [msg.payload for msg in result_grouper.msgs if not msg.is_eof()]
        self.assertEqual(len(payloads), 1)  # One message 
        self.assertEqual(len(payloads[0]), 1)  # One joined row
