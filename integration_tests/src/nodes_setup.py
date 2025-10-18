
from middleware.mocks.middleware import IntermediateMiddleware

from middleware.memory_middleware import * 
from middleware.routing.csv_message import * 
from middleware.routing.query_types import * 

from selectnode.src.select_type_config import *
from selectnode.src.selectnode import *
from selectnode.src.config_init import *

from groupbynode.src.groupbynode import *
from groupbynode.src.groupby_initialize import *
from groupbynode.src.topk_initialize import *

from groupbynode.src.topk_initialize import *
from joinnode.src.config_init import *
from joinnode.src.joinnode import *

from common.config.type_expander import *


class NodesSetup:
    def __init__(self, msg_type, select_middleware, join_middleware, groupby_middleware, topk_middleware, result_middleware):
        self.select_middleware= select_middleware
        self.join_middleware= join_middleware
        self.groupby_middleware= groupby_middleware
        self.topk_middleware= topk_middleware
        self.result_middleware= result_middleware
        self.msg_type = msg_type

    def get_select_node(self, msg_type = None):
        types_expander = TypeExpander()
        add_selectnode_config(types_expander, self.result_middleware, self.groupby_middleware)
        if msg_type:
            return SelectNode(self.select_middleware, msg_type, types_expander)

        return SelectNode(self.select_middleware, self.msg_type, types_expander)

    def get_join_node(self, msg_type = None):
        types_expander = TypeExpander()
        add_joinnode_config(types_expander, self.result_middleware, self.join_middleware)

        if msg_type:
            return JoinNode(self.join_middleware, msg_type, types_expander)
            
        return JoinNode(self.join_middleware, self.msg_type, types_expander)


    def get_topk_node(self, msg_type = None):
        config = configure_types_topk(self.join_middleware)
        if msg_type:
            return GroupbyNode(self.topk_middleware, msg_type, config)
            
        return GroupbyNode(self.topk_middleware, self.msg_type, config)

    def get_groupby_node(self, msg_type = None):
        config = configure_types_groupby(
                self.join_middleware, self.topk_middleware) #topk_middleware_type = HashedMemoryMessageBuilder

        if msg_type:
            return GroupbyNode(self.groupby_middleware, msg_type, config)
            
        return GroupbyNode(self.groupby_middleware, self.msg_type, config)
        

    def close(self):
        self.select_middleware.close()
        self.join_middleware.close()
        self.groupby_middleware.close()
        self.topk_middleware.close()
        self.result_middleware.close()
