import unittest
import time
from common.state_storage import query_state_storage as q_state
from common.state_storage.base_state_manager import BaseStateManager

class StatObject:
    def __init__(self):
        self.st_mtime = 0
        self.count_modified_time_stamp = 0
        self.count_written_times = 0

    def update_mtime(self):
        self.st_mtime = time.time()


class MockPath:
    def __init__(self, fs, path):
        self.fs = fs
        self.path = path
        self.content = b""
        self.name = self.fs.get_name(self)

    def __truediv__(self, new_segment):
        # Simulate directory traversal or path extension
        new_path = self.path + "/" + new_segment
        return MockPath(self.fs, new_path)

    def get_children(self):
        return self.fs.get_children(self)
    def glob(self, pattern):
        return list(self.get_children().values())

    def touch(self):
        if self.path not in self.fs.paths:
            self.fs.paths[self.path] = self
            self.fs.update_mod_time(self.path)
        else:
            self.fs.add_count_modified_time_stamp(self.path)

        # Add to parent directory if not already there
        parent = self.fs.get_parent(self.path)
        if parent:
            children = parent.get_children()
            if self.path not in children:
                children[self.path] = self

    def rmchildren(self):
        for child_path, child in self.get_children().items():
            child.rmchildren()
            self.fs.delete_path(child)
        self.fs.reset_children(self)

    def mkdir(self, parents=False, exist_ok=False):
        # Create directories
        if self.path not in self.fs.paths:
            self.fs.paths[self.path] = self
        # Add to parent directory if not already there
        parent = self.fs.get_parent(self.path)
        if parent:
            parent.get_children()[self.path] = self

    def exists(self):
        return self.path in self.fs.paths

    def unlink(self):
        # Simulate file deletion
        self.fs.delete_path(self)
        parent = self.fs.get_parent(self.path)
        if parent:
            children = parent.get_children()
            if self.path in children:
                del children[self.path]

    def write_bytes(self, data):
        self.content = data
        self.fs.paths[self.path] = self  # Update the filesystem with new content
        self.fs.add_count_written(self.path)

    def read_bytes(self):
        return self.content

    def __repr__(self):
        return self.path

    def replace(self, new_path):
        self.unlink()
        self.path = str(new_path)
        self.name = self.fs.get_name(self)
        self.touch()

    def stat(self):
        return self.fs.stats[self.path]



class MockFilesystem:
    def __init__(self):
        self.paths = {}
        self.deleted_paths ={}
        self.stats = {}
        self.children = {}

    def reset_children(self, path_obj):
        self.children[path_obj.path]=  {}

    def get_children(self, path_obj):
        return self.children.setdefault(path_obj.path, {})

    def update_mod_time(self, path):
        obj = self.stats.setdefault(path, StatObject())
        obj.update_mtime()

    def add_count_modified_time_stamp(self, path):
        obj = self.stats.setdefault(path, StatObject())
        obj.count_modified_time_stamp+=1
        obj.update_mtime()

    def add_count_written(self, path):
        obj = self.stats.setdefault(path, StatObject())
        obj.count_written_times+=1
        obj.update_mtime()

    def delete_path(self, path_obj):
        if path_obj.path in self.paths:
            del self.paths[path_obj.path]
            stat = self.stats.setdefault(path_obj.path, StatObject())
            del self.stats[path_obj.path]

            if path_obj.path in self.children:
                del self.stats[path_obj.path]

            
            deleted_times = self.deleted_paths.setdefault(path_obj.path, [])
            deleted_times.append((path_obj, stat))


    def open_file(self, path, mode):
        if path in self.paths:
            return self.paths[path]
        raise FileNotFoundError(f"{path} not found.")

    def clear_directory(self, path):
        # Clean up all files in a directory (recursively)
        self.paths[path.path].rmchildren()

    def create_new_path(self, path):
        path_obj = MockPath(self, path)
        self.paths[path] = path_obj
        return path_obj

    def get_parent(self, path):
        # Simple method to return the parent directory of a path (based on '/')
        parts = path.strip('/').split('/')
        if len(parts) > 1:
            parent_path = '/'.join(parts[:-1])
            return self.paths.get(parent_path)
        return None

    def get_name(self, path):
        parts = path.path.strip('/').split('/')
        return parts[-1]

class MockStateManager(BaseStateManager):
    def apply_changes(self, state, changes):
        return state+changes

    def serialize_state(self, state):
        return str(state).encode()
    def serialize_changes(self, changes):
        return "\n".join([str(change) for change in  changes]).encode()

    def deserialize_state(self, state_bytes):
        return state_bytes.decode()

    def deserialize_changes(self, changes_bytes):
        return changes_bytes.decode()



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

        state_storage.register_packet("q1", 1)
        exp_file = "initial_folder/states/q1_0"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 0)

        exp_file = "initial_folder/metadata/q1_commit"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 0)

    def test_simple_query_state_commit(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_packet("q1", 1)
        state_storage.commit_changes("q1")

        exp_file = "initial_folder/metadata/q1_commit"
        self.assertIn(exp_file, self.mock_fs.paths)
        file_stat = self.mock_fs.stats[exp_file]

        self.assertEqual(file_stat.count_modified_time_stamp, 1 )
        self.assertEqual(file_stat.count_written_times, 0)

    def test_simple_query_state_double_register_no_modify(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)


        state_storage.register_packet("q1", 1)
        state_storage.register_packet("q1", 1)

        exp_file = "initial_folder/states/q1_0"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 0)

        exp_file = "initial_folder/metadata/q1_commit"
        self.assertIn(exp_file, self.mock_fs.paths)

        file_stat = self.mock_fs.stats[exp_file]
        self.assertEqual(file_stat.count_modified_time_stamp, 0 )
        self.assertEqual(file_stat.count_written_times, 0)


    def test_simple_query_state_push_changes_fails_because_changes_file_does_not_exist(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_packet("q1", 1)
        state_storage.commit_changes("q1")

        exp_file = "initial_folder/states/q1_1"
        exp_file2 = "initial_folder/packets/finished/q1_1"
        temp_file = "initial_folder/packets/not_finished/q1_1"

        self.assertRaises(q_state.InvalidStateError, state_storage.push_changes, "q1", 1, "SOME NEW STATE!")

    def test_simple_query_state_push_changes(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_packet("q1", 1)
        state_storage.commit_changes("q1")

        exp_file = "initial_folder/states/q1_1"
        exp_file2 = "initial_folder/packets/finished/q1_1"
        temp_file = "initial_folder/packets/not_finished/q1_1"

        self.mock_fs.create_new_path("initial_folder/packets/not_applied/q1_1")

        state_storage.push_changes("q1", 1, "SOME NEW STATE!")

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



    def test_simple_query_state_check_integrity_apply_commited(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_packet("q1", 1)
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


    def test_simple_query_state_check_integrity_discard_apply_not_commited(self):
        state_manager= MockStateManager()
        state_storage = q_state.QueryStateStorage("initial_folder", state_manager)

        state_storage.register_packet("q1", 1)
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
