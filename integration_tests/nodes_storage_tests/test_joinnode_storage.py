import unittest


from common.state_storage import query_state_storage as q_state

from common.state_storage.base_state_manager import BaseStateManager


from integration_tests.src.mocks_fs import *

from middleware.mocks.middleware import *
from common.config.row_mapping import *
from common.config.type_expander import *

from middleware.rabbitmq import utils as rbmq_utils
from integration_tests.src.mocks_rabbit import *

from middleware.join_tasks_middleware import *


from middleware.routing.header_fields import BaseHeaders

from joinnode.src.config_init import *
from joinnode.src.joinnode import *
from joinnode.src.join_state_manager import JoinNodeStateManager
from joinnode.src.join_accumulator import JoinAccumulator

from common.config.row_joining import *
from joinnode.src.join_type_config import *

def copied_row_with(base_row, **kwargs):
    res = {}

    for key, itm in base_row.items():
        res[key] = itm 

    for key, itm in kwargs.items():
        res[key] = itm 

    return res


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


class TestJoinNodeStorage(unittest.TestCase):

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
        in_left = ["product_id","product_name"]
        in_right = ["top_product_id","month","revenue",]
        out_cols = ["product_id","product_name","month","revenue",]

        def create(headers):  
            
            res= BareMockMessageBuilderNoSerial(headers, headers.ids[0])
            res.headers.types[0] = "q2_OUT"
            return res
        config = JoinTypeConfiguration(result_grouper, create,
            left_type= "q2.LEFT", #
            in_fields_left=in_left,  # ..product names
            in_fields_right=in_right,
            join_id = "q2_OUT",
            join_conf=[INNER_ON_EQ, {"col_left":"product_id", "col_right":"top_product_id"}],
            out_cols= out_cols
        )


        type_exp = TypeExpander()

        type_exp.add_configuration_to_many(config, "q2.LEFT", "q2.RIGHT")

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


    def get_simple_messages_q2(self, query_id, count_left = 1, count_right = 1):
        # Returns the messages themselves, last message is eof.. and expected output
        in_left = ["product_id","product_name"]
        in_right = ["top_product_id","month","revenue",]
        out_cols = ["product_id","product_name","month","revenue",]
        
        rows_left = [
            {"product_name":"PRODUCT NAME 1", "product_id": "prod_1_{msg_id}"},
            {"product_name":"PRODUCT NAME 2", "product_id": "prod_2_{msg_id}"},
            {"product_name":"PRODUCT NAME 3", "product_id": "prod_3_{msg_id}"},
        ]

        rows_right = [
            {"top_product_id": "prod_1_{msg_id}", "month": 1, "revenue": 10},
            {"top_product_id": "prod_1_{msg_id}", "month": 2, "revenue": 20},
            {"top_product_id": "prod_2_{msg_id}", "month": 3, "revenue": 30},
            {"top_product_id": "prod_4_{msg_id}", "month": 4, "revenue": 40},
        ]

        expected_out = [
            {"product_id": "prod_1_{msg_id}", "product_name": "PRODUCT NAME 1", "month": 1, "revenue": 10},
            {"product_id": "prod_1_{msg_id}", "product_name": "PRODUCT NAME 1", "month": 2, "revenue": 20},
            {"product_id": "prod_2_{msg_id}", "product_name": "PRODUCT NAME 2", "month": 3, "revenue": 30},
        ]
        messages_left = []

        # rows_left = [map_dict_to_vect_cols(in_left, row) for row in rows_left]
        # rows_right = [map_dict_to_vect_cols(in_right, row) for row in rows_right]

        map_f_left = lambda r: map_dict_to_vect_cols(in_left, r)
        for i in range(0, count_left):
            messages_left.append(BareMockMessageBuilderNoSerial.for_payload(
                    [query_id],
                    ["q2.LEFT"],
                    list(copied_row_with(row, product_id = row["product_id"].format(msg_id = i)) for row in rows_left)
                    , map_f_left,packet_id = i)
            )

        eof_message_left = BareMockMessageBuilderNoSerial.for_payload([query_id],["q2.LEFT"],[], map_f_left, packet_id = count_left) 
        eof_message_left.set_as_eof()


        outputs_after_right = []
        messages_right = []

        map_f_right = lambda r: map_dict_to_vect_cols(in_right, r)
        for i in range(0, count_right):
            messages_right.append(BareMockMessageBuilderNoSerial.for_payload(
                    [query_id],
                    ["q2.RIGHT"],
                    list(copied_row_with(row, top_product_id = row["top_product_id"].format(msg_id = i)) for row in rows_right)
                    , map_f_right,packet_id = i)
            )

            new_out = []

            # for res_i in range(0, i+1):
            res_i = i
            for res_row in expected_out:
                new_out.append(
                    map_dict_to_vect_cols(out_cols,
                    copied_row_with(res_row, product_id = res_row["product_id"].format(msg_id = res_i))
                    )
                )

            outputs_after_right.append(new_out)


        eof_message_right = BareMockMessageBuilderNoSerial.for_payload([query_id],["q2.RIGHT"],[], map_f_right, packet_id = count_right) 
        eof_message_right.set_as_eof()

        return messages_left, eof_message_left, messages_right, eof_message_right, outputs_after_right


    def creator_storage(self,state_handler):
        return q_state.QueryStateStorage("initial_folder", state_handler)

    def push_msg(self, middleware, builder):
        #f"tag_{builder.headers.packet_id}"
        middleware.send(builder)

    def get_join_middleware(self):
        return JoinTasksMiddleware(1, ind = 0)

    def get_res_middleware(self):
        return MockCopyMiddleware()

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
            return JoinAccumulator(None, BareMockMessageBuilderNoSerial.default(), ide =acc_id)

        state_storage = q_state.QueryStateStorage("initial_folder", JoinNodeStateManager(get_accum_mock))

        self.assertIn("initial_folder", self.mock_fs.paths)
        self.assertIn("initial_folder/states", self.mock_fs.paths)
        self.assertIn("initial_folder/metadata", self.mock_fs.paths)
        self.assertIn("initial_folder/versions", self.mock_fs.paths)


    def test_query_2_joinode_with_storage_no_recovery_single_msg(self):


        # In order
        count_messages_left = 1
        count_messages_right = 1
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_join_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        count_messages = count_messages_left + count_messages_right +1
        self.set_register_tags(count_messages+1) # +1 for right eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = JoinNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, limit = 3)
        node.start()


        messages_left, eof_message_left, messages_right, eof_message_right, outputs_after_right = self.get_simple_messages_q2(q_id, count_left = count_messages_left, count_right = count_messages_right)
        messages = messages_left+messages_right

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2_OUT"), list(outputs_after_right) # i.e groupby only sends one message to output with the content of the last one.
            )

        tags_acked = []
        i = 0
        for message in messages_left:
            self.push_msg(in_middle, message)

            tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

        self.push_msg(in_middle, eof_message_left)
        tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack
        i+=1

        for message in messages_right:
            self.push_msg(in_middle, message)

            tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

            # Check internal storage state == exp_outs[i]

        self.push_msg(in_middle, eof_message_right)

        tags_acked.append(self.get_def_tag(count_messages))
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof
        self.assertEqual(len(result_grouper.msgs), len(expected_out_msgs) +1) # +1 cuz of eofs

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[ind].payload]
            self.assertEqual(got_result, exp_content, f"At msg {ind}")




    def test_query_2_joinode_with_storage_no_recovery_many_msgs(self):

        # In order
        count_messages_left = 2
        count_messages_right = 2
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_join_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        count_messages = count_messages_left + count_messages_right +1
        self.set_register_tags(count_messages+1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = JoinNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, limit = 3)
        node.start()


        messages_left, eof_message_left, messages_right, eof_message_right, outputs_after_right = self.get_simple_messages_q2(q_id, count_left = count_messages_left, count_right = count_messages_right)
        messages = messages_left+messages_right

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2_OUT"), list(outputs_after_right) # i.e groupby only sends one message to output with the content of the last one.
            )

        tags_acked = []
        i = 0
        for message in messages_left:
            self.push_msg(in_middle, message)

            tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

        self.push_msg(in_middle, eof_message_left)
        tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack
        i+=1

        for message in messages_right:
            self.push_msg(in_middle, message)

            tags_acked.append(self.get_def_tag(i)) # Ack is by batches of ... 1.. so each message should add last ack

            self.assertEqual(list(conn_mock.iter_acked()), tags_acked) 
            
            i+=1

            # Check internal storage state == exp_outs[i]
        self.push_msg(in_middle, eof_message_right)

        tags_acked.append(self.get_def_tag(count_messages))
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof
        self.assertEqual(len(result_grouper.msgs), len(expected_out_msgs) +1) # +1 cuz of eofs

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[ind].payload]
            self.assertEqual(got_result, exp_content, f"At msg {ind}")


    def test_query_2_joinode_with_storage_no_recovery_but_batch_size_ack(self):

        # In order
        count_messages_left = 5
        count_messages_right = 5
        batch_size = 2
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_join_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        count_messages = count_messages_left + count_messages_right +1
        self.set_register_tags(count_messages+1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = JoinNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size, limit=3) # Each 2 messages 
        node.start()


        messages_left, eof_message_left, messages_right, eof_message_right, outputs_after_right = self.get_simple_messages_q2(q_id, count_left = count_messages_left, count_right = count_messages_right)
        messages = messages_left+messages_right

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2_OUT"), list(outputs_after_right) # i.e groupby only sends one message to output with the content of the last one.
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

        self.push_msg(in_middle, eof_message_left)
        self.push_msg(in_middle, eof_message_right)

        # Eof makes ack of remaining.. 
        for tag_id in range(last_start, count_messages+1):
            tags_acked.append(self.get_def_tag(tag_id)) # Ack is by batches of ... 1.. so each message should add last ack
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[ind].payload]
            # i = 0
            # for elem in exp_content:
            #     self.assertEqual(got_result[i], elem)
            #     i += 1

            self.assertEqual(got_result, exp_content, f"At msg {ind}")
            # self.assertEqual(len(got_result), len(exp_content), f"At msg {ind}")

        self.assertEqual(len(result_grouper.msgs), len(expected_out_msgs) +1) # +1 cuz of eofs




    def test_query_2_joinode_with_storage_recovery_and_batch_size_ack(self):
        # In order
        count_messages_left = 5
        count_messages_right = 5
        batch_size = 2
        fail_at_msg = 4 # Just before the second batch save i.e msg 3 was lost

        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_join_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        count_messages = count_messages_left + count_messages_right +1
        self.set_register_tags(count_messages+1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = JoinNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size, limit=3) # Each 2 messages 
        node.start()


        messages_left, eof_message_left, messages_right, eof_message_right, outputs_after_right = self.get_simple_messages_q2(q_id, count_left = count_messages_left, count_right = count_messages_right)
        messages = messages_left+messages_right

        # Use headers of

        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2_OUT"), list(outputs_after_right) # i.e groupby only sends one message to output with the content of the last one.
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
        
        node = JoinNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size, limit=3) # Each 2 messages 
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

        self.push_msg(in_middle, eof_message_left)
        self.push_msg(in_middle, eof_message_right)

        # node.describe()

        # Eof makes ack of remaining.. 
        for tag_id in range(last_start, count_messages+1):
            tags_acked.append(self.get_def_tag(tag_id)) # Ack is by batches of ... 1.. so each message should add last ack
        
        self.assertEqual(list(conn_mock.iter_acked()), tags_acked)

        # self.assertEqual(len(result_grouper.msgs), count_messages +1) # Include eof

        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[ind].payload]
            self.assertEqual(got_result, exp_content, f"At msg {ind}")

        self.assertEqual(len(result_grouper.msgs), len(expected_out_msgs) +1) # +1 cuz of eofs

    def test_query_2_joinode_with_no_recovery_but_batch_size_store_of_states(self):

        # In order
        count_messages_left = 5
        count_messages_right = 5
        batch_size = 2
        q_id = "query1_1" # 1 after _ is used for hashing!

        result_grouper = self.get_res_middleware()
        in_middle = self.get_join_middleware()

        conf_map = self.get_q2_conf(result_grouper)


        count_messages = count_messages_left + count_messages_right +1
        self.set_register_tags(count_messages+1) # +1 for eof


        conn_mock = self.active_conns.get(rbmq_utils.RABBITMQ_HOST, None)

        node = JoinNode(in_middle, MockMessage, conf_map, store_creator = self.creator_storage, batch_size = batch_size, limit=1000) # Each 2 messages 
        node.start()


        messages_left, eof_message_left, messages_right, eof_message_right, outputs_after_right = self.get_simple_messages_q2(q_id, count_left = count_messages_left, count_right = count_messages_right)
        messages = messages_left+messages_right

        # For retrieving states saved in fs
        def get_accum_mock(acc_id):
            return JoinAccumulator(conf_map.type_configurations[0], BareMockMessageBuilderNoSerial.default(), ide =acc_id)
        retrieve_storage = self.creator_storage(JoinNodeStateManager(get_accum_mock))


        i = 0
        for message in messages_left:
            self.push_msg(in_middle, message)
            i+=1

        self.push_msg(in_middle, eof_message_left)
        left_end_i = i+1
        i=0

        exp_outs = []
        for message in messages_right:
            self.push_msg(in_middle, message)
            exp_outs = exp_outs + outputs_after_right[i] # Append extra res/joined

            if (i+1) % batch_size == 0: #Saves state after batch size msgs
                saved_states = retrieve_storage.load_states()

                self.assertEqual(len(saved_states), 1) # Just this query!
                acc_id = node.get_acc_id(q_id, "q2_OUT")

                self.assertIn(acc_id, saved_states)

                q2_version, q2_state = saved_states[acc_id]


                exp_ver = int((left_end_i+i+1)/batch_size)# Count of full batch sizes cycles

                exp_ver= exp_ver * batch_size # For now version includes/is not normalized by batch size
                
                self.assertEqual(q2_version, exp_ver) 

                # print(f"----> Q2 {i} {exp_ver} state left rows", q2_state.left_rows)
                # print("----> Q2 state right rows", q2_state.right_rows)
                # print("----> Q2 state out rows", q2_state.get_out_rows())
                # And state partial result should be the same
                partial = node.get_partial_result_from(q2_state).serialize_payload()

                self.assertEqual(partial, exp_outs) 
            i+=1

            # Check internal storage state == exp_outs[i]

        self.push_msg(in_middle, eof_message_right)


        self.assertEqual(len(result_grouper.msgs), 1+1) # 1 message since limit is so high and +1 cuz of eofs


        expected_out_msgs, expected_eof = self.get_simple_expected_out(
                self.get_headers(q_id, "q2_OUT"), [exp_outs] # i.e groupby only sends one message to output with the content of the last one.
            )


        for ind, (exp_headers, exp_content) in enumerate(expected_out_msgs):
            self.assertEqual(
                result_grouper.msgs[ind].headers.to_dict(), 
                exp_headers.to_dict())

            got_result = [x for x in result_grouper.msgs[ind].payload]
            self.assertEqual(got_result, exp_content, f"At msg {ind}")

