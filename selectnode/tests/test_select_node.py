import unittest
from selectnode.src.selectnode import * 
from selectnode.src.row_filtering import * 

class MockSender:
    def __init__(self):
        self.msgs= []
    def send_msg(self, msg):
        self.msgs.append(msg)

class MockChannel:
    def __init__(self, result_sender):
        self.handler = None
        self.result_sender = result_sender

    def consume_select_tasks(self, handler):
        self.handler = handler

    def start_consume(self):
        pass

    def push_msg(self, msg):
        self.handler(msg)


    def open_sender_to_results(self):
        return self.result_sender

class MockConn:
    def __init__(self, channel):
        self.channel = channel

    def open_channel(self):
        return self.channel


class MockMessageBuilder:
    def __init__(self,query_id, query_type):
        self.id = query_id
        self.type = query_type
        self.payload = []
        self.fields= []

    def set_fields(self, fields):
        # set field names!
        self.fields = fields

    def add_row(self,row):
        #assert len(row) == len(fields) # Same size of fields 
        self.payload.append(row)

    def build(self):
        return MockMessage("", [self.id], [self.type], self.payload)

class MockMessage:
    def __init__(self, tag, queries_id, queries_type, payload):
        self.tag = tag
        self.ids = queries_id
        self.types = queries_type
        self.payload = payload
        # assert len of queries == len of types!
        assert len(self.ids) == len(self.types)

    def len_queries(self):
        return len(self.ids)

    def clone_with(self, queries_id, queries_type):
        return MockMessage(self.tag, queries_id, queries_type, self.payload)


    def result_builder_for_single(self, ind):
        return MockMessageBuilder(self.ids[ind],self.types[ind])


    # For subclasses
    def ack_self(self):
        pass

    def describe(self):
        pass

    def stream_rows(self):
        return iter(self.payload)


class TestSelectNode(unittest.TestCase):
    def test_query_1_selectnode(self):
        filters_serial = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
        ]

        type_map = {"query_t_1": filters_serial}

        result_grouper = MockSender()
        channel_mock = MockChannel(result_grouper)

        node = SelectNode(MockConn(channel_mock), type_map)

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

        channel_mock.push_msg(message);
        self.assertTrue(len(result_grouper.msgs)==1)

        got_result = result_grouper.msgs[0].payload

        for elem in rows_pass:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass))


    def test_multiquery_selectnode(self):
        filters_serial = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["sum", GREATER_THAN_OP, [75]],
        ]

        filters_serial2 = [
                ["year", EQUALS_ANY, [2024, 2025]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
        ]

        type_map = {"query_t_1": filters_serial, "query_t_2": filters_serial2}

        result_grouper = MockSender()
        channel_mock = MockChannel(result_grouper)

        node = SelectNode(MockConn(channel_mock), type_map)

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

        channel_mock.push_msg(message);
        self.assertTrue(len(result_grouper.msgs)==2)

        # CHECK STILL QUERY 1 is solved
        got_result = result_grouper.msgs[0].payload

        self.assertTrue(result_grouper.msgs[0].ids[0] == "query_3323")
        self.assertTrue(result_grouper.msgs[0].types[0] == "query_t_1")
        self.assertTrue(len(result_grouper.msgs[0].ids) == 1)
        
        for elem in rows_pass:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass))


        # CHECK QUERY 2
        got_result = result_grouper.msgs[1].payload
        self.assertTrue(result_grouper.msgs[1].types[0] == "query_t_2")


        rows_pass2 = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]
        for elem in rows_pass2:
            self.assertTrue(elem in got_result)

        self.assertTrue(len(got_result) == len(rows_pass2))
