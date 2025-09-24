import unittest
from selectnode.src.row_filtering import * 
from middleware.routing.select_task_message import * 
from middleware.routing.result_message import * 

class MethodClass:
    def __init__(self, tag):
        self.delivery_tag =tag
class TestSelectTask(unittest.TestCase):

    def test_serial_deserial_select(self):
        msg_build = SelectTaskMessageBuilder(["8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5"], ["query_1"])

        rows_pass = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
            {'year': 2027, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 24, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]

        for itm in rows_pass:
            msg_build.add_row(itm)

        payload = msg_build.serialize_payload()
        res_msg = SelectTaskMessage(None, MethodClass("Something"), msg_build.get_headers(), payload)
        res_msg.describe()
        res = [itm for itm in res_msg.stream_rows()]

        self.assertTrue(len(rows_pass) == len(res))

        for i in range(len(rows_pass)):
            self.assertTrue(rows_pass[i] == res[i])


    def test_serial_deserial_result(self):
        msg_build = ResultMessageBuilder(["8845cdaa-d230-4453-bbdf-0e4f783045bf,76.5"], ["query_1"])

        rows_pass = [
            {'year': 2024, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 23, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 942},
            {'year': 2027, 'hour': 7, 'sum': 88},
            {'year': 2025, 'hour': 24, 'sum': 942},
            {'year': 2024, 'hour': 6, 'sum': 55},
        ]

        for itm in rows_pass:
            msg_build.add_row(itm)

        payload = msg_build.serialize_payload()
        res_msg = ResultMessage(None, MethodClass("Something"), msg_build.get_headers(), payload)
        res_msg.describe()
        res = [itm for itm in res_msg.stream_rows()]

        self.assertTrue(len(rows_pass) == len(res))

        for i in range(len(rows_pass)):
            self.assertTrue(rows_pass[i] == res[i])


