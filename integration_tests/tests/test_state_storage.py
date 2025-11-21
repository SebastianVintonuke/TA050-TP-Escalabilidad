import unittest
from common.state_storage import ack_query_state_storage as q_state
from common.state_storage.base_state_manager import BaseStateManager
from integration_tests.src.mocks_fs import *


## Single/ batch size == 1 state manager
NOT_REGISTERED_PACKET = "NOT_REGISTERED_PACKET"
class MockStateManager(BaseStateManager):
    def __init__(self, acks = []):
        self.acks = acks
        self.ind_acked = 0
    def apply_changes(self, state, changes):
        return state+changes

    def serialize_state(self, state):
        return str(state).encode()
    def serialize_changes(self, changes):
        return "\n".join([str(change) for change in  changes]).encode()

    def deserialize_state(self, state_bytes):
        return state_bytes.decode()

    def deserialize_changes(self, changes_bytes):
        if self.ind_acked < len(self.acks):
            ack_tag = self.acks[self.ind_acked]
            self.ind_acked +=1
            return changes_bytes.decode(), [ack_tag]
        return changes_bytes.decode(), [NOT_REGISTERED_PACKET]

class TestStateStorage(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)



    def setUp(self):
        self.mock_fs = MockFilesystem()
        q_state.PathType = self.mock_fs.create_new_path
        q_state.open_file = self.mock_fs.open_file
        q_state.clear_directory = self.mock_fs.clear_directory
    
    def test_simple_query_state_storage_creates_dirs(self):
        state_storage = q_state.QueryStateStorage("initial_folder", MockStateManager())

        self.assertIn("initial_folder", self.mock_fs.paths)
        self.assertIn("initial_folder/states", self.mock_fs.paths)
        self.assertIn("initial_folder/metadata", self.mock_fs.paths)
        self.assertIn("initial_folder/packets", self.mock_fs.paths)


    def test_simple_query_state_save_changes(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.write_changes("q1","1", ["CHANGE1", "CHANGE2"])
        exp_file = "initial_folder/packets/not_applied/q1_1"
        temp_file = "initial_folder/packets/not_finished/q1_1"

        ## Assert file for packets and query is on not applied
        self.assertIn(exp_file, self.mock_fs.paths)

        ## Assert file in not finished was indeed there but deleted
        self.assertNotIn(temp_file, self.mock_fs.paths)
        self.assertIn(temp_file, self.mock_fs.deleted_paths)
        deleted_files = self.mock_fs.deleted_paths[temp_file]
        self.assertEqual(len(deleted_files), 1) # just once!

        file = self.mock_fs.paths[exp_file]

        self.assertEqual(file.content, state_manager.serialize_changes(["CHANGE1", "CHANGE2"]))
        self.assertEqual(file.content, deleted_files[0][0].content) # Deleted temp file at the end had the same content!



    def test_simple_query_state_register_packet_creates_files_for_query(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        exp_file = "initial_folder/states/q1_0"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 1)

        exp_file = "initial_folder/metadata/q1_commit"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 0)

    def test_simple_query_state_commit(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        state_storage.commit_changes("q1")

        exp_file = "initial_folder/metadata/q1_commit"
        self.assertIn(exp_file, self.mock_fs.paths)
        file_stat = self.mock_fs.stats[exp_file]

        self.assertEqual(file_stat.count_modified_time_stamp, 1 )
        self.assertEqual(file_stat.count_written_times, 0)

    def test_simple_query_state_double_register_no_modify(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)


        state_storage.register_query("q1", {})
        state_storage.register_query("q1", {})

        exp_file = "initial_folder/states/q1_0"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 1) # It did in fact... write initial state for register query.. once

        exp_file = "initial_folder/metadata/q1_commit"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 0)


    def test_simple_query_state_push_changes_fails_because_changes_file_does_not_exist(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        state_storage.commit_changes("q1")

        self.assertRaises(q_state.InvalidStateError, state_storage.push_changes, "q1", 1, "SOME NEW STATE!", ["msg1"])

    def test_simple_query_state_push_changes(self):
        exp_acks = ["msg1"]
        state_manager= MockStateManager(acks = exp_acks)
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        state_storage.commit_changes("q1")

        exp_file = "initial_folder/states/q1_1"

        exp_file2 = "initial_folder/packets/finished/msg1"
        # exp_file2 = "initial_folder/packets/finished/q1_1"
        temp_file = "initial_folder/packets/not_finished/q1_1"

        self.mock_fs.create_new_path("initial_folder/packets/not_applied/q1_1")

        state_storage.push_changes("q1", 1, "SOME NEW STATE!", exp_acks)

        ## Assert file for packets and query is on not applied
        self.assertIn(exp_file, self.mock_fs.paths)

        ## Assert file in not finished was indeed there but deleted
        self.assertNotIn(temp_file, self.mock_fs.paths)
        self.assertIn(temp_file, self.mock_fs.deleted_paths)
        deleted_files = self.mock_fs.deleted_paths[temp_file]
        self.assertEqual(len(deleted_files), 1) # just once!

        file = self.mock_fs.paths[exp_file]

        self.assertEqual(file.content, state_manager.serialize_state("SOME NEW STATE!"))
        self.assertEqual(file.content, deleted_files[0][0].content) # Deleted temp file at the end had the same content!

        self.assertIn(exp_file2, self.mock_fs.paths)
        ## NOW Lets check ack?
        acked = []
        state_storage.ack_finished(acked.append)
        self.assertEqual(acked,exp_acks)

        #Once again to check it wont do it twice.
        state_storage.ack_finished(acked.append)
        self.assertEqual(acked, exp_acks)        



    def test_simple_query_state_check_integrity_apply_commited(self):
        state_manager= MockStateManager(acks = ["msg1"])
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        state_storage.commit_changes("q1")

        exp_file = "initial_folder/states/q1_1"
        temp_file = "initial_folder/packets/not_applied/q1_1"

        state_storage.write_changes("q1","1", ["CHANGE1", "CHANGE2"])
        state_storage.commit_changes("q1")

        # Recreate/simulate a crash?/reset?
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.check_integrity()

        ## Assert file for packets and query is on not applied
        self.assertIn(exp_file, self.mock_fs.paths)



        ## Assert file in not finished was indeed there but deleted
        self.assertNotIn(temp_file, self.mock_fs.paths)
        self.assertIn(temp_file, self.mock_fs.deleted_paths)
        deleted_files = self.mock_fs.deleted_paths[temp_file]
        self.assertEqual(len(deleted_files), 1) # just once!

        deleted_f, stat = deleted_files[0]
        file = self.mock_fs.paths[exp_file]

        self.assertEqual(file.content, state_manager.serialize_state("CHANGE1\nCHANGE2"))
        self.assertEqual(file.content, deleted_f.content) # Deleted temp file at the end had the same content!

        ## NOW Lets check ack?
        acked = []
        state_storage.ack_finished(acked.append)
        self.assertEqual(["msg1"], acked)
        
        #Once again to check it wont do it twice.
        state_storage.ack_finished(acked.append)
        self.assertEqual(["msg1"], acked)


    def test_simple_query_state_check_integrity_discard_apply_not_commited(self):
        state_manager= MockStateManager(acks = ["msg1"])
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        state_storage.commit_changes("q1")

        exp_file = "initial_folder/states/q1_1"
        temp_file = "initial_folder/packets/not_applied/q1_1"

        state_storage.write_changes("q1","1", ["CHANGE1", "CHANGE2"])
        # Recreate/simulate a crash?/reset?
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.check_integrity()

        ## Assert file for packets and query is on not applied
        self.assertNotIn(exp_file, self.mock_fs.paths)




        ## Assert file in not finished was indeed there but deleted
        self.assertNotIn(temp_file, self.mock_fs.paths)
        self.assertIn(temp_file, self.mock_fs.deleted_paths)
        deleted_files = self.mock_fs.deleted_paths[temp_file]
        self.assertEqual(len(deleted_files), 1) # just once!
