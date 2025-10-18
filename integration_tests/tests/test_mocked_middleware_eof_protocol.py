import unittest


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

class TestRealMiddlewaresEOFProtocol(unittest.TestCase, BaseEOFProtocolTest):
    def get_node_setup(self):
        return BaseEOFProtocolTest.wrap_intermediate(
            msg_type= CSVMessage,# Initial msg_type
            select_middleware = SerializeMemoryMiddleware(),
            join_middleware = SerializeMemoryMiddleware(),
            groupby_middleware = SerializeMemoryMiddleware(),
            topk_middleware = SerializeMemoryMiddleware(),
            result_middleware = SerializeMemoryMiddleware(),
        )

