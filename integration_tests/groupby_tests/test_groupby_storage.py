import unittest


from common.state_storage import query_state_storage as q_state
from common.state_storage.base_state_manager import BaseStateManager

from groupbynode.src.groupby_state_manager import GroupbyStateManager

from integration_tests.src.mocks_fs import *

from middleware.mocks.middleware import *
from common.config.row_mapping import *
from common.config.type_expander import *

from groupbynode.src.row_aggregate import *
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


class TestGroupbyStorage(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.mock_fs = MockFilesystem()
        q_state.PathType = self.mock_fs.create_new_path
        q_state.open_file = self.mock_fs.open_file
        q_state.clear_directory = self.mock_fs.clear_directory
    
    def test_simple_query_state_storage_creates_dirs(self):

        req_ids = []

        def get_accum_mock(acc_id):
            req_ids.append(acc_id)
            return None

        state_storage = q_state.QueryStateStorage("initial_folder", GroupbyStateManager(get_accum_mock))

        self.assertIn("initial_folder", self.mock_fs.paths)
        self.assertIn("initial_folder/states", self.mock_fs.paths)
        self.assertIn("initial_folder/metadata", self.mock_fs.paths)
        self.assertIn("initial_folder/packets", self.mock_fs.paths)


    def creator_storage(self,state_handler):
        return q_state.QueryStateStorage("initial_folder", state_handler)

    def test_simple_query_state_storage_creates_dirs(self):

        req_ids = []

        def get_accum_mock(acc_id):
            req_ids.append(acc_id)
            return None

        state_storage = q_state.QueryStateStorage("initial_folder", GroupbyStateManager(get_accum_mock))

        self.assertIn("initial_folder", self.mock_fs.paths)
        self.assertIn("initial_folder/states", self.mock_fs.paths)
        self.assertIn("initial_folder/metadata", self.mock_fs.paths)
        self.assertIn("initial_folder/packets", self.mock_fs.paths)

    def test_query_2_groupbynode_with_storage_no_recovery(self):

        # In order
        in_cols = ["product_id", "month", "revenue"]
        out_cols = ["product_id", "month", "revenue", "quantity_sold"]


        result_grouper = MockMiddlewareTags()
        type_conf = GroupbyTypeConfiguration(result_grouper, BareMockMessageBuilder, 
                in_fields = in_cols, #EQUALS to out cols from select node main 
                grouping_conf = [["product_id", "month"], [
                    [SUM_ACTION,"revenue"],
                    [COUNT_ACTION, "quantity_sold"],
                ]],
                out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )


        in_middle = MockMiddlewareTags()

        type_exp= {
            "t1": type_conf
        }



        node = GroupbyNode(in_middle, MockMessage, type_exp, store_creator = self.creator_storage)
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
        message = BareMockMessageBuilder.for_payload(
            ["query_3323"],
            ["t1"],
            rows,map_f,
        )

        in_middle.push_msg(message, "TAG_MSG_1")
        self.assertEqual(in_middle.acked_messages, ["TAG_MSG_1"]) # Ensure acked after processing message or so.

        # eof
        eof_message = BareMockMessageBuilder.for_payload(["query_3323"],["t1"],[], map_f) 
        eof_message.set_as_eof(1)
        in_middle.push_msg(eof_message, "TAG_MSG_EOF")

        self.assertEqual(in_middle.acked_messages, ["TAG_MSG_1", "TAG_MSG_EOF"]) # Ensure acked after processing message or so.
        

        self.assertEqual(len(result_grouper.msgs), message.headers.len_queries() *2) # Include eof for each type

        for ind, exp_out_headers in enumerate(message.headers.split()):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_out_headers.to_dict())

        #self.assertEqual(result_grouper.msgs[0].msg_from, message)

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result), len(expected))
        ind = 0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind += 1





    ####
    #### This just creates about the same message it sends it and essnetially it should be saved on self.mock_fs
    #### As mock_fs resets only in a per test basis.
    def simple_q2_message_on_fs(self, result_grouper, in_middle):
        in_cols = ["product_id", "month", "revenue"]
        out_cols = ["product_id", "month", "revenue", "quantity_sold"]

        type_conf = GroupbyTypeConfiguration(result_grouper, BareMockMessageBuilder, 
                in_fields = in_cols, #EQUALS to out cols from select node main 
                grouping_conf = [["product_id", "month"], [
                    [SUM_ACTION,"revenue"],
                    [COUNT_ACTION, "quantity_sold"],
                ]],
                out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )



        type_exp= {
            "t1": type_conf
        }

        node = GroupbyNode(in_middle, MockMessage, type_exp, store_creator = self.creator_storage)
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
        message = BareMockMessageBuilder.for_payload(
            ["query_3323"],
            ["t1"],
            rows,map_f,
        )

        in_middle.push_msg(message, "TAG_MSG_1")
        return type_conf, map_f

    def test_query_2_groupbynode_with_storage_recovery(self):

        # In order
        in_middle = MockMiddlewareTags()
        result_grouper = MockMiddlewareTags()

        type_conf,map_f = self.simple_q2_message_on_fs(result_grouper, in_middle)

        self.assertEqual(in_middle.acked_messages, ["TAG_MSG_1"]) # First run actually acked message.

        type_exp= {
            "t1": type_conf
        }

        node = GroupbyNode(in_middle, MockMessage, type_exp, store_creator = self.creator_storage)

        ## Check no state was loaded?
        node.start()

        ## Check state was loaded? Or just send message
        self.assertEqual(len(result_grouper.msgs), 0) # No message was sent to result since no eof yet..

        # eof
        eof_message = BareMockMessageBuilder.for_payload(["query_3323"],["t1"],[], map_f) 
        eof_message.set_as_eof(1)
        in_middle.push_msg(eof_message, "TAG_MSG_EOF")

        self.assertEqual(in_middle.acked_messages, ["TAG_MSG_1", "TAG_MSG_EOF"]) # Ensure acked after processing message or so.

        self.assertEqual(len(result_grouper.msgs),2) # 1 message with payload and eof.

        for ind, exp_out_headers in enumerate(message.headers.split()):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_out_headers.to_dict())

        #self.assertEqual(result_grouper.msgs[0].msg_from, message)

        got_result = [x for x in result_grouper.msgs[0].payload]
        self.assertEqual(len(got_result), len(expected))
        ind = 0
        for elem in expected:
            self.assertEqual(got_result[ind], elem)
            ind += 1


