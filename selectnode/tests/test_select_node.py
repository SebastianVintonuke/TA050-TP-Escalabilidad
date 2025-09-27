import unittest

from selectnode.src.row_filtering import * 
from selectnode.src.select_type_config import * 
from selectnode.src.selectnode import * 
from selectnode.src.mocks_middleware import * 


def map_dict_to_vect(row):
    return [
        row["year"], row["hour"], row["sum"]
    ]
def map_vect_to_dict(row):
    return {'year': int(row[0]), 'hour': int(row[1]), 'sum': int(row[2])}


class TestSelectNode(unittest.TestCase):

    def test_query_1_selectnode(self):
        in_fields = ["year", "hour", "sum"]
        filters_serial = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
        ]
        result_grouper = MockMiddleware()
        in_middle = MockMiddleware()

        type_map = {
            "query_t_1": SelectTypeConfiguration(result_grouper, MockMessageBuilder, in_fields = in_fields, filters_conf=filters_serial)
        }

        node = SelectNode(in_middle, type_map)
        node.start()
        
        rows_pass = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
        ]
        rows_fail = [
            {'year': 2027, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 24, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]

        message = MockMessage("tag1",["query_3323"], ["query_t_1"],
            rows_pass+ rows_fail, map_dict_to_vect
        )

        in_middle.push_msg(message);

        self.assertTrue(len(result_grouper.msgs)==1)
        self.assertTrue(result_grouper.msgs[0].ind == 0)
        self.assertTrue(result_grouper.msgs[0].msg_from == message)

        got_result = [map_vect_to_dict(el) for el in result_grouper.msgs[0].payload]

        for elem in rows_pass:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass))

    def test_multiquery_selectnode(self):
        in_fields = ["year", "hour", "sum"]
        filters_serial = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
        ]

        filters_serial2 = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
        ]

        result_grouper = MockMiddleware()
        in_middle = MockMiddleware()

        type_map = {
            "query_t_1": SelectTypeConfiguration(result_grouper, MockMessageBuilder, in_fields =in_fields,filters_conf = filters_serial),
            "query_t_2": SelectTypeConfiguration(result_grouper, MockMessageBuilder, in_fields =in_fields,filters_conf = filters_serial2)
        }

        node = SelectNode(in_middle, type_map)
        node.start()

        rows_pass = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
        ]
        rows_fail = [
            {'year': 2027, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 24, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]

        message = MockMessage("tag1",["query_3323", "query_3342"], ["query_t_1", "query_t_2"],
            rows_pass+ rows_fail, map_dict_to_vect
        )

        in_middle.push_msg(message);
        self.assertTrue(len(result_grouper.msgs)==2)

        # CHECK STILL QUERY 1 is solved
        self.assertTrue(result_grouper.msgs[0].ind == 0)
        self.assertTrue(result_grouper.msgs[0].msg_from == message)

        got_result = [map_vect_to_dict(el) for el in result_grouper.msgs[0].payload]

        for elem in rows_pass:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass))


        # CHECK QUERY 2
        self.assertTrue(result_grouper.msgs[1].ind == 1)
        self.assertTrue(result_grouper.msgs[1].msg_from == message)
        got_result = [map_vect_to_dict(el) for el in result_grouper.msgs[1].payload]

        rows_pass2 = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]
        for elem in rows_pass2:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass2))
