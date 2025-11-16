import unittest
from common.state_storage import query_state_storage as q_state
from common.state_storage.base_state_manager import BaseStateManager
from groupbynode.src.groupby_state_manager import GroupbyStateManager
from integration_tests.src.mocks_fs import *

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

