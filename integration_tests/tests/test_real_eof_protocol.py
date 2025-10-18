import unittest

from middleware.mocks.middleware import *

from common.config.row_filtering import *
from common.config.row_mapping import *
from common.config.type_expander import *
from selectnode.src.select_type_config import *
from selectnode.src.selectnode import *
from selectnode.src.config_init import *

from groupbynode.src.groupbynode import *
from groupbynode.src.groupby_initialize import *
from groupbynode.src.topk_initialize import *

from groupbynode.src.topk_initialize import *
from joinnode.src.config_init import *
from joinnode.src.joinnode import *

from middleware.memory_middleware import * 
from middleware.routing.csv_message import * 
from middleware.routing.query_types import * 

from middleware.rabbitmq import utils as rbmq_utils
from integration_tests.src.mocks_rabbit import *
from integration_tests.tests.base_test_eof_protocol import *


from middleware.select_tasks_middleware import *
from middleware.join_tasks_middleware import *
from middleware.groupby_middleware import *
from middleware.result_node_middleware import *

def real_setup():
    return BaseEOFProtocolTest.wrap_intermediate(
        msg_type= CSVMessage,# Initial msg_type
        select_middleware = SelectTasksMiddleware(),
        join_middleware = JoinTasksMiddleware(1, ind = 0),
        groupby_middleware = GroupbyTasksMiddleware(1, ind = 0),
        topk_middleware = SerializeMemoryMiddleware(),
        result_middleware = ResultNodeMiddleware(),
    )

class TestRealMiddlewaresEOFProtocol(unittest.TestCase, BaseEOFProtocolTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.active_conns = {}


    def get_node_setup(self):
        return real_setup()

    def mock_open_connection(self, host, attempts):
        res = MockConnection(host)
        self.active_conns[host] = res

        return res

    def setUp(self):
        rbmq_utils.try_open_connection = self.mock_open_connection
        rbmq_utils.build_headers = PropHeaders
        rbmq_utils.wait_middleware_init = wait_middleware_init_nothing
