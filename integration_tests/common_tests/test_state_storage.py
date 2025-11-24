import unittest
from common.state_storage import query_state_storage as q_state
from common.state_storage.base_state_manager import BaseStateManager
from integration_tests.src.mocks_fs import *

def expected_deleted_file(file, times, cont_getter = None):
    return (file, times, cont_getter)

def expected_file(file, times_written= None, times_modified = None, cont_getter = None):
    return (file, cont_getter, times_written, times_modified)


## Single/ batch size == 1 state manager
NOT_REGISTERED_PACKET = "NOT_REGISTERED_PACKET"
class MockStateManager(BaseStateManager):
    def __init__(self, acks = []):
        self.acks = acks
        self.ind_acked = 0
    def apply_changes(self, state, changes):
        return state+self.serialize_changes(changes).decode()

    def serialize_state(self, state):
        return str(state).encode()
    def serialize_changes(self, changes):
        return "\n".join([str(change) for change in  changes]).encode()

    def deserialize_state(self, state_bytes):
        return state_bytes.decode()

    def deserialize_changes(self, changes_bytes):
        changes = changes_bytes.decode().split("\n")
        return changes, 1

class TestStateStorage(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    def assert_files_existance(self, files_exist, files_deleted):

        existent_paths = set(self.mock_fs.paths.keys())

        for file_path, content_exp_getter, times_written, times_modified in files_exist:
            self.assertIn(file_path, existent_paths)

            file_stat = self.mock_fs.stats[file_path]
            file = self.mock_fs.paths[file_path]

            if times_written:
                self.assertEqual(file_stat.count_written_times, times_written, f"In file {file_path}")

            if times_modified:
                self.assertEqual(file_stat.count_modified_time_stamp, times_modified, f"In file {file_path}")

            if content_exp_getter:
                self.assertEqual(file.content, content_exp_getter(), f"In file {file_path}") # Assert deleted temp file == expected


        deleted_paths = set(self.mock_fs.deleted_paths.keys())

        for file_path, times, content_exp_getter in files_deleted:
            self.assertNotIn(file_path, existent_paths)
            self.assertIn(file_path, deleted_paths)
            deleted_files = self.mock_fs.deleted_paths[file_path]

            self.assertEqual(len(deleted_files), times, f"In file {file_path}") #assert was deleted N times

            if content_exp_getter:
                self.assertEqual(deleted_files[-1][0].content, content_exp_getter(), f"In file {file_path}") # Assert deleted temp file == expected

    def get_content_of(self, file):
        return self.mock_fs.paths[file].content

    def cont_same_as_exp(self, expected_file):
        return lambda: self.get_content_of(expected_file[0]) # [0] == path of expected.

    def cont_equals(self, content):
        return lambda: content
    
    def setUp(self):
        self.mock_fs = MockFilesystem()
        q_state.PathType = self.mock_fs.create_new_path
        q_state.open_file = self.mock_fs.open_file
        q_state.copy_file = self.mock_fs.copy_file
        q_state.clear_directory = self.mock_fs.clear_directory
    
    def test_simple_query_state_storage_creates_dirs(self):
        state_storage = q_state.QueryStateStorage("initial_folder", MockStateManager())

        self.assertIn("initial_folder", self.mock_fs.paths)
        self.assertIn("initial_folder/states", self.mock_fs.paths)
        self.assertIn("initial_folder/metadata", self.mock_fs.paths)
        self.assertIn("initial_folder/versions", self.mock_fs.paths)


    def test_simple_query_state_register_query_creates_files_for_query(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        exp_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/states/q1_0", cont_getter= exp_cont_getter, times_written = 1, times_modified = 0), 
            expected_file("initial_folder/metadata/q1_commit", cont_getter= exp_cont_getter, times_written = 0, times_modified = 0),
        ]
        exp_deleted_files = [
        ]

        #Act
        state_storage.register_query("q1", {})

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)

    def test_simple_cancel_logic(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        exp_cont_getter = self.cont_equals(b"")
        state_storage.register_query("q1", "")
        state_storage.register_query("q2", "")
        state_storage.cancel_query("q1")

        self.assertEqual(state_storage.is_cancelled_query("q1"), True)
        self.assertEqual(state_storage.is_query_registered("q1"), True) # Not deleted really just marked as cancelled

        self.assertEqual(state_storage.is_cancelled_query("q2"), False)# Obviously q2 still is fine!
        self.assertEqual(state_storage.is_query_registered("q2"), True) 


    def test_simple_query_state_unregister_query_deletes_files_for_query(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        exp_cont_getter = self.cont_equals(b"")
        
        exp_files = [
        ]
        exp_deleted_files = [
            expected_deleted_file("initial_folder/states/q1_0", cont_getter= exp_cont_getter, times = 1),
            expected_deleted_file("initial_folder/metadata/q1_commit", cont_getter= exp_cont_getter, times = 1),
        ]

        #Act
        state_storage.register_query("q1", {})
        state_storage.unregister_query("q1")

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)

    def test_simple_query_state_unregister_query_deletes_files_for_query_but_keeps_other_query(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        exp_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/states/q2_0", cont_getter= exp_cont_getter, times_written = 1, times_modified = 0),
            expected_file("initial_folder/metadata/q2_commit", cont_getter= exp_cont_getter, times_written = 0, times_modified = 0),
        ]
        exp_deleted_files = [
            expected_deleted_file("initial_folder/states/q1_0", cont_getter= exp_cont_getter, times = 1),
            expected_deleted_file("initial_folder/metadata/q1_commit", cont_getter= exp_cont_getter, times = 1),
        ]

        #Act
        state_storage.register_query("q1", {})
        state_storage.register_query("q2", {})
        state_storage.unregister_query("q1")

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)


    def test_simple_query_state_commit_updates_timestamp(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        exp_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/states/q1_0", cont_getter= exp_cont_getter, times_written = 1, times_modified = 0),
            expected_file("initial_folder/metadata/q1_commit", cont_getter= exp_cont_getter, times_written = 0, times_modified = 1),
        ]
        exp_deleted_files = [
        ]

        #Act
        state_storage.register_query("q1", {})
        state_storage.commit_changes("q1")

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)

    def test_simple_query_state_double_register_no_modify(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        exp_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/states/q1_0", cont_getter= exp_cont_getter, times_written = 1, times_modified = 0),
            expected_file("initial_folder/metadata/q1_commit", cont_getter= exp_cont_getter, times_written = 0, times_modified = 0),
        ]
        exp_deleted_files = [
        ]

        #Act
        state_storage.register_query("q1", {})
        state_storage.register_query("q1", {})

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)


    def test_simple_query_state_push_changes_fails_because_changes_file_does_not_exist(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        self.assertRaises(q_state.InvalidStateError, state_storage.push_changes, "q1", 1, "SOME NEW STATE!", 1)

    def test_simple_get_new_state_fails_because_base_version_does_not_exist(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_query("q1", {})
        self.assertRaises(q_state.InvalidStateError, state_storage.get_new_state, "q1", 2, "SOME NEW STATE!", 1)




    def test_simple_query_state_save_changes(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        changes = ["CHANGE1", "CHANGE2"]

        exp_cont_getter = self.cont_equals(state_manager.serialize_changes(changes))
        exp_files = [
            expected_file("initial_folder/versions/not_applied/q1_1", cont_getter= exp_cont_getter),
        ]
        exp_deleted_files = [
            expected_deleted_file("initial_folder/versions/not_finished/q1_1", times = 1, cont_getter = self.cont_same_as_exp(exp_files[0]))
        ]

        #Act
        state_storage.write_changes("q1", 1, changes)

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)

    def test_simple_query_state_push_changes_from_memory(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        changes = ["CHANGE1", "CHANGE2"]

        new_state = state_manager.serialize_changes(changes)

        exp_cont_getter = self.cont_equals(new_state)
        empty_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/states/q1_1", cont_getter= exp_cont_getter, times_written = 0, times_modified = 0), # New state was saved, but actually was not written directly, was moved!            
            expected_file("initial_folder/metadata/q1_commit", cont_getter= empty_cont_getter, times_written = 0, times_modified = 0), # Not modified commit time here.
        ]
        exp_deleted_files = [
            expected_deleted_file("initial_folder/versions/not_finished/q1_1", times = 2, cont_getter = self.cont_same_as_exp(exp_files[0])), # 2 times deleted one for write and one for push.
            expected_deleted_file("initial_folder/states/q1_0", times = 1), # Prev state was deleted
            expected_deleted_file("initial_folder/versions/not_applied/q1_1",times = 1), #not applied was deleted too at the end
        ]

        #Act
        state_storage.register_query("q1", "")
        state_storage.write_changes("q1", 1, changes)

        # No commit needed for when pushing changes from memory
        state_storage.push_changes("q1", 1, new_state.decode()) # Base ver == 0 i.e new_ver =1 .. 1-1 == 0

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)

    def test_simple_query_state_push_changes_from_state(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        changes = ["CHANGE1", "CHANGE2"]

        new_state = state_manager.serialize_changes(changes)

        exp_cont_getter = self.cont_equals(new_state)
        empty_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/states/q1_1", cont_getter= exp_cont_getter, times_written = 0, times_modified = 0), # New state was saved, but actually was not written directly, was moved!            
            expected_file("initial_folder/metadata/q1_commit", cont_getter= empty_cont_getter, times_written = 0, times_modified = 0), # Not modified commit time here.
        ]
        exp_deleted_files = [
            expected_deleted_file("initial_folder/versions/not_finished/q1_1", times = 2, cont_getter = self.cont_same_as_exp(exp_files[0])), # 2 times deleted one for write and one for push.
            expected_deleted_file("initial_folder/states/q1_0", times = 1), # Prev state was deleted
            expected_deleted_file("initial_folder/versions/not_applied/q1_1",times = 1), #not applied was deleted too at the end
        ]

        #Act
        state_storage.register_query("q1", "")
        state_storage.write_changes("q1", 1, changes)

        new_state_res = state_storage.get_new_state("q1",1, changes, 1) # Use ver 0 as base and calc from 'disk'/backedup
        # No commit needed for when pushing changes from memory
        state_storage.push_changes("q1", 1, new_state_res) # Base ver == 0 i.e new_ver =1 .. 1-1 == 0

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)

    def test_simple_query_state_check_integrity_apply_commited_resumed_all_ok(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        changes = ["CHANGE1", "CHANGE2"]

        new_state = state_manager.serialize_changes(changes)

        exp_cont_getter = self.cont_equals(new_state)
        empty_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/states/q1_1", cont_getter= exp_cont_getter, times_written = 0, times_modified = 0), # New state was saved, but actually was not written directly, was moved!            
            expected_file("initial_folder/metadata/q1_commit", cont_getter= empty_cont_getter, times_written = 0, times_modified = 1), # Modified commit time here.
        ]
        exp_deleted_files = [
            expected_deleted_file("initial_folder/versions/not_finished/q1_1", times = 2, cont_getter = self.cont_same_as_exp(exp_files[0])), # 2 times deleted one for write and one for push.
            expected_deleted_file("initial_folder/states/q1_0", times = 1), # Prev state was deleted
            expected_deleted_file("initial_folder/versions/not_applied/q1_1",times = 1), #not applied was deleted too at the end
        ]

        #Act
        state_storage.register_query("q1", "")
        state_storage.write_changes("q1", 1, changes)
        state_storage.commit_changes("q1")

        ## SOMETHING CRASHED! And recreated
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)
        state_storage.check_integrity()

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)


    def test_simple_query_state_check_integrity_discards_not_commited(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        changes = ["CHANGE1", "CHANGE2"]

        new_state = state_manager.serialize_changes(changes)

        exp_cont_getter = self.cont_equals(new_state)
        empty_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/metadata/q1_commit", cont_getter= empty_cont_getter, times_written = 0, times_modified = 0), # Modified commit time here.
            expected_file("initial_folder/states/q1_0", cont_getter= empty_cont_getter, times_written = 1, times_modified = 0),
     
        ]
        exp_deleted_files = [
            # Not applied so not finished was created only once!
            expected_deleted_file("initial_folder/versions/not_finished/q1_1", times = 1, cont_getter = exp_cont_getter), # 2 times deleted one for write and one for push.
            expected_deleted_file("initial_folder/versions/not_applied/q1_1",times = 1), #not applied was deleted too at the end
        ]

        #Act
        state_storage.register_query("q1", "")
        state_storage.write_changes("q1", 1, changes)

        ## SOMETHING CRASHED! And recreated
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)
        state_storage.check_integrity()

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)



    def test_simple_query_state_check_integrity_discards_when_gap_in_versions(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        # Arrange
        changes = ["CHANGE1", "CHANGE2"]

        new_state = state_manager.serialize_changes(changes)

        exp_cont_getter = self.cont_equals(new_state)
        empty_cont_getter = self.cont_equals(b"")
        
        exp_files = [
            expected_file("initial_folder/metadata/q1_commit", cont_getter= empty_cont_getter, times_written = 0, times_modified = 0), # Modified commit time here.
            expected_file("initial_folder/states/q1_0", cont_getter= empty_cont_getter, times_written = 1, times_modified = 0),
     
        ]
        exp_deleted_files = [
            # Not applied so not finished was created only once!
            expected_deleted_file("initial_folder/versions/not_finished/q1_2", times = 1, cont_getter = exp_cont_getter), # 2 times deleted one for write and one for push.
            expected_deleted_file("initial_folder/versions/not_applied/q1_2",times = 1), #not applied was deleted too at the end
        ]

        #Act
        state_storage.register_query("q1", "")
        state_storage.write_changes("q1", 2, changes)
        state_storage.commit_changes("q1")

        ## SOMETHING CRASHED! And recreated
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)
        state_storage.check_integrity()

        #Assert
        self.assert_files_existance(exp_files, exp_deleted_files)






"""
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

"""