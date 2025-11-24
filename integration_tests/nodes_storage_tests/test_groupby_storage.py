import unittest


from common.state_storage import query_state_storage as q_state

from common.state_storage.base_state_manager import BaseStateManager

from groupbynode.src.groupby_state_manager import GroupbyStateManager, QueryAccumulator

from integration_tests.src.mocks_fs import *

from middleware.mocks.middleware import *
from common.config.row_mapping import *
from common.config.type_expander import *

from groupbynode.src.row_aggregate import *
from groupbynode.src.row_grouping import *

from groupbynode.src.groupby_type_config import *
from groupbynode.src.groupbynode import *


from middleware.rabbitmq import utils as rbmq_utils
from integration_tests.src.mocks_rabbit import *

from middleware.groupby_middleware import *


from middleware.routing.header_fields import BaseHeaders

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
        self.active_conns = {}



    ##
    ## Setup and so on functions for tests
    ##

    def get_headers(self, query_id, q_type):
        return  BaseHeaders(
            [query_id],
            [q_type])

    def set_register_tags(self, count_messages):
        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)
        self.assertNotEqual(conn_mock, None)
        self.assertEqual(len(self.active_conns) ,1)

        tags = [self.get_def_tag(i) for i in range(count_messages)]
        conn_mock.register_tags(tags)

    def get_def_tag(self, i):
        return f"tag_{i}"



    def get_q2_conf(self, result_grouper):
        in_cols = ["product_id", "month", "revenue"]
        out_cols = ["product_id", "month", "revenue", "quantity_sold"]

        type_conf = GroupbyTypeConfiguration(result_grouper, lambda headers: BareMockMessageBuilderNoSerial(headers, headers.ids[0]), 
                in_fields = in_cols, #EQUALS to out cols from select node main 
                grouping_conf = [["product_id", "month"], [
                    [SUM_ACTION,"revenue"],
                    [COUNT_ACTION, "quantity_sold"],
                ]],
                out_conf={ROW_CONFIG_OUT_COLS: out_cols},
        )

        type_exp= {
            "q2": type_conf
        }

        return type_exp


    def get_simple_expected_out(self, base_headers, contents):
        packet_id = 0
        res_exp = []
        count = len(contents)

        for i in range(count):
            
            exp = base_headers.clone()
            exp.packet_id = packet_id
            res_exp.append((exp, contents[i]))
            packet_id+=1

        # Add expected eof

        eof_exp = base_headers.clone()
        eof_exp.packet_id = packet_id
        eof_exp.msg_count= count

        return res_exp, eof_exp




    def get_simple_messages_q2(self, query_id, count = 1):
        # Returns the messages themselves, last message is eof.. and expected output
        in_cols = ["product_id", "month", "revenue"]

        rows = [
            {"product_id": "pr1", "month": 7, "revenue": 88},
            {"product_id": "pr1", "month": 7, "revenue": 88},
            {"product_id": "pr1", "month": 8, "revenue": 10},
            
            {"product_id": "pr2", "month": 7, "revenue": 942},
            {"product_id": "pr2", "month": 23, "revenue": 942},
            
            {"product_id": "pr3", "month": 6, "revenue": 942},
        ]

        expected_step = [
            ["pr1", "7", 176.0, 2],
            ["pr1", "8", 10.0, 1],
            ["pr2", "7", 942.0, 1],
            ["pr2", "23", 942.0, 1],
            ["pr3", "6", 942.0, 1],
        ]

        messages = []
        outputs_mess = []

        map_f = lambda r: map_dict_to_vect_cols(in_cols, r)
        
        for i in range(1, count+1):
            messages.append(BareMockMessageBuilderNoSerial.for_payload(
                    [query_id],
                    ["q2"],
                    rows, map_f,packet_id = i-1)
            )

            new_out = []
            for res_row in expected_step:
                new_out.append(
                    [res_row[0], res_row[1], str(res_row[2]*i), str(res_row[3]*i)]
                )

            outputs_mess.append(new_out)


        eof_message = BareMockMessageBuilderNoSerial.for_payload([query_id],["q2"],[], map_f, packet_id = count) 
        eof_message.set_as_eof(count)

        return messages, outputs_mess, eof_message


    def creator_storage(self,state_handler):
        return q_state.QueryStateStorage("initial_folder", state_handler)

    def push_msg(self, middleware, builder):
        #f"tag_{builder.headers.packet_id}"
        middleware.send(builder)

    def get_groupby_middleware(self):
        return GroupbyTasksMiddleware(1, ind = 0)

    def get_res_middleware(self):
        return MockMiddlewareTags()

    def mock_open_connection(self, host, attempts):
        res = MockConnection(host)
        self.active_conns[host] = res

        return res

    def setUp(self):
        self.mock_fs = MockFilesystem()
        q_state.PathType = self.mock_fs.create_new_path
        q_state.open_file = self.mock_fs.open_file
        q_state.copy_file = self.mock_fs.copy_file
        q_state.clear_directory = self.mock_fs.clear_directory


        rbmq_utils.try_open_connection = self.mock_open_connection
        rbmq_utils.build_headers = PropHeaders
        rbmq_utils.wait_middleware_init = wait_middleware_init_nothing






    ##
    ## Actual tests!
    ##
    def test_simple_query_state_storage_creates_dirs(self):

        req_ids = []

        def get_accum_mock(acc_id):
            req_ids.append(acc_id)
            return QueryAccumulator(acc_id, None, BareMockMessageBuilderNoSerial.default())

        state_storage = q_state.QueryStateStorage("initial_folder", GroupbyStateManager(get_accum_mock))

        self.assertIn("initial_folder", self.mock_fs.paths)
        self.assertIn("initial_folder/states", self.mock_fs.paths)
        self.assertIn("initial_folder/metadata", self.mock_fs.paths)
        self.assertIn("initial_folder/versions", self.mock_fs.paths)


    def test_query_2_groupbynode_with_storage_no_recovery_single_msg(self):

        # In order
        count_messages = 1
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_groupby_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        self.set_register_tags(count_messages +1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = GroupbyNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage)
        node.start()


        messages, exp_outs, eof_message = self.get_simple_messages_q2(q_id, count = count_messages)

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2"), [exp_outs[-1]] # i.e groupby only sends one message to output with the content of the last one.
            )

        tags_acked = []
        i = 0
        for message in messages:
            self.push_msg(in_middle, message)

            tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

            # Check internal storage state == exp_outs[i]

        self.push_msg(in_middle, eof_message)

        tags_acked.append(self.get_def_tag(eof_message.headers.packet_id))
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof
        self.assertEqual(len(result_grouper.msgs), 2) # Groupby only sends 1 message for content a 1 eof, per type so q2 that expands to two types in out would be 2*2

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[0].payload]
            ind = 0
            for elem in exp_content:
                self.assertEqual(got_result[ind], elem)
                ind += 1

            self.assertEqual(len(got_result), len(exp_content))




    def test_query_2_groupbynode_with_storage_no_recovery_many_msgs(self):

        # In order
        count_messages = 2
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_groupby_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        self.set_register_tags(count_messages +1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = GroupbyNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage)
        node.start()


        messages, exp_outs, eof_message = self.get_simple_messages_q2(q_id, count = count_messages)

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2"), [exp_outs[-1]] # i.e groupby only sends one message to output with the content of the last one.
            )

        tags_acked = []
        i = 0
        for message in messages:
            self.push_msg(in_middle, message)

            tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

            # Check internal storage state == exp_outs[i]

        self.push_msg(in_middle, eof_message)

        tags_acked.append(self.get_def_tag(eof_message.headers.packet_id))
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof
        self.assertEqual(len(result_grouper.msgs), 2) # Groupby only sends 1 message for content a 1 eof, per type so q2 that expands to two types in out would be 2*2

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[0].payload]
            ind = 0
            for elem in exp_content:
                self.assertEqual(got_result[ind], elem)
                ind += 1

            self.assertEqual(len(got_result), len(exp_content))


    def test_query_2_groupbynode_with_storage_no_recovery_but_batch_size_ack(self):

        # In order
        count_messages = 5
        batch_size = 2
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_groupby_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        self.set_register_tags(count_messages +1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = GroupbyNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size) # Each 2 messages 
        node.start()


        messages, exp_outs, eof_message = self.get_simple_messages_q2(q_id, count = count_messages)

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2"), [exp_outs[-1]] # i.e groupby only sends one message to output with the content of the last one.
            )

        tags_acked = []
        i = 0
        last_start = 0
        for message in messages:
            self.push_msg(in_middle, message)

            if (i+1) % batch_size == 0:
                curr_end = last_start+ batch_size
                for tag_id in range(last_start, curr_end):
                    tags_acked.append(self.get_def_tag(tag_id)) # Ack is by batches of ... 1.. so each message should add last ack
                last_start = curr_end

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

            # Check internal storage state == exp_outs[i]

        self.push_msg(in_middle, eof_message)

        # Eof makes ack of remaining.. 
        for tag_id in range(last_start, count_messages+1):
            tags_acked.append(self.get_def_tag(tag_id)) # Ack is by batches of ... 1.. so each message should add last ack
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof
        self.assertEqual(len(result_grouper.msgs), 2) # Groupby only sends 1 message for content a 1 eof, per type so q2 that expands to two types in out would be 2*2

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[0].payload]
            ind = 0
            for elem in exp_content:
                self.assertEqual(got_result[ind], elem)
                ind += 1

            self.assertEqual(len(got_result), len(exp_content))




    def test_query_2_groupbynode_with_no_recovery_but_batch_size_store_of_states(self):

        # In order
        count_messages = 5
        batch_size = 2
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_groupby_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        self.set_register_tags(count_messages +1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = GroupbyNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size) # Each 2 messages 
        node.start()


        messages, exp_outs, eof_message = self.get_simple_messages_q2(q_id, count = count_messages)

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2"), [exp_outs[-1]] # i.e groupby only sends one message to output with the content of the last one.
            )


        # For retrieving states saved in fs
        def get_accum_mock(acc_id):
            return QueryAccumulator(acc_id, conf_map["q2"], BareMockMessageBuilderNoSerial.default())
        retrieve_storage = self.creator_storage(GroupbyStateManager(get_accum_mock))


        i = 0

        for message in messages:
            self.push_msg(in_middle, message)

            if (i+1) % batch_size == 0: #Saves state after batch size msgs
                saved_states = retrieve_storage.load_states()

                self.assertEqual(len(saved_states), 1) # Just this query!
                acc_id = node.get_acc_id(q_id, "q2")

                self.assertIn(acc_id, saved_states)

                q2_version, q2_state = saved_states[acc_id]


                exp_ver = int((i+1)/batch_size)# Count of full batch sizes cycles

                exp_ver= exp_ver * batch_size # For now version includes/is not normalized by batch size
                
                self.assertEqual(q2_version, exp_ver) 

                # And state partial result should be the same
                partial = node.get_partial_result_from(q2_state).serialize_payload()

                self.assertEqual(partial, exp_outs[i]) 

            i+=1

            # Check internal storage state == exp_outs[i]

        self.push_msg(in_middle, eof_message)


        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof
        self.assertEqual(len(result_grouper.msgs), 2) # Groupby only sends 1 message for content a 1 eof, per type so q2 that expands to two types in out would be 2*2

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[0].payload]
            ind = 0
            for elem in exp_content:
                self.assertEqual(got_result[ind], elem)
                ind += 1

            self.assertEqual(len(got_result), len(exp_content))






    def test_query_2_groupbynode_with_storage_recovery_and_batch_size_ack(self):

        # In order
        count_messages = 5
        batch_size = 2
        fail_at_msg = 4 # Just before the second batch save i.e msg 3 was lost

        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_groupby_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        self.set_register_tags(count_messages +1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = GroupbyNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size) # Each 2 messages 
        node.start()


        messages, exp_outs, eof_message = self.get_simple_messages_q2(q_id, count = count_messages)

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2"), [exp_outs[-1]] # i.e groupby only sends one message to output with the content of the last one.
            )

        tags_acked = []
        i = 0
        last_start = 0
        for message in messages[:fail_at_msg]: #  Only send first messages up to fail/crash
            self.push_msg(in_middle, message)

            if (i+1) % batch_size == 0:
                curr_end = last_start+ batch_size
                for tag_id in range(last_start, curr_end):
                    tags_acked.append(self.get_def_tag(tag_id)) # Ack is by batches of ... 1.. so each message should add last ack
                last_start = curr_end

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

            # Check internal storage state == exp_outs[i]

        ### CRASHED or so... so re start/recreate process
        start_ind_msgs = len(tags_acked) # If acked messages then do not resend.
        
        node = GroupbyNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size) # Each 2 messages 
        node.start()

        # Now resume sending and so on..
        i = start_ind_msgs
        last_start = start_ind_msgs
        for message in messages[start_ind_msgs:]: #  Only after started
            self.push_msg(in_middle, message)
            if (i+1) % batch_size == 0:
                curr_end = last_start+ batch_size
                for tag_id in range(last_start, curr_end):
                    tags_acked.append(self.get_def_tag(tag_id)) # Ack is by batches of ... 1.. so each message should add last ack
                last_start = curr_end

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

        self.push_msg(in_middle, eof_message)

        # Eof makes ack of remaining.. 
        for tag_id in range(last_start, count_messages+1):
            tags_acked.append(self.get_def_tag(tag_id)) # Ack is by batches of ... 1.. so each message should add last ack
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof
        self.assertEqual(len(result_grouper.msgs), 2) # Groupby only sends 1 message for content a 1 eof, per type so q2 that expands to two types in out would be 2*2

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[0].payload]
            ind = 0
            for elem in exp_content:
                self.assertEqual(got_result[ind], elem)
                ind += 1

            self.assertEqual(len(got_result), len(exp_content))


