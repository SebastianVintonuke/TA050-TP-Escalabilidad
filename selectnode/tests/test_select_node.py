import unittest

from selectnode.src.row_filtering import * 
from selectnode.src.type_config import * 
from selectnode.src.selectnode import * 

from middleware.middleware import * 
from middleware.routing.message import * 



def map_dict_to_vect(row):
    return [
        row["year"], row["hour"], row["sum"]
    ]
def map_vect_to_dict(row):
    return {'year': int(row[0]), 'hour': int(row[1]), 'sum': int(row[2])}


class MockMiddleware(MessageMiddleware):
    def __init__(self):
        self.msgs= []
        self.callback = None
    def send(self, msg):
        self.msgs.append(msg)

    def push_msg(self, msg):
        self.callback(msg)

    def start_consuming(self, on_message_callback):
        self.callback = on_message_callback

    def stop_consuming(self):
        pass
    def close(self):
        pass

    def delete(self):
        pass


class MockMessageBuilder:
    def __init__(self,msg, ind):
        self.msg_from = msg
        self.ind= ind
        self.payload = []
        self.fields= []

    def set_fields(self, fields):
        # set field names!
        self.fields = fields

    def add_row(self,row):
        #assert len(row) == len(fields) # Same size of fields 
        self.payload.append(row)

class MockMessage(Message):
    def __init__(self, tag, queries_id, queries_type, payload):
        super().from_data(tag,queries_type,queries_type, payload)
    def _deserialize_payload(self, payload): # Do nothing with it.
        return payload

    def clone_with(self, queries_id, queries_type):
        return MockMessage(self.tag, queries_id, queries_type, self.payload)

    def stream_rows(self):
        return map(map_dict_to_vect, iter(self.payload))




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
            "query_t_1": TypeConfiguration(result_grouper, MockMessageBuilder, in_fields = in_fields, filters_conf=filters_serial)
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
            rows_pass+ rows_fail
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
            "query_t_1": TypeConfiguration(result_grouper, MockMessageBuilder, in_fields =in_fields,filters_conf = filters_serial),
            "query_t_2": TypeConfiguration(result_grouper, MockMessageBuilder, in_fields =in_fields,filters_conf = filters_serial2)
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
            rows_pass+ rows_fail
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
