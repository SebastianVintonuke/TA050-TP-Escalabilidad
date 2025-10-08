
from middleware.errors import * 
from middleware.routing.query_types import *


from middleware.routing.csv_message import CSVMessageBuilder,CSVHashedMessageBuilder

from .groupby_type_config import * 
from .row_aggregate import * 

def create_diverged_message(msg, ind):
    return csv_message.CSVHashedMessageBuilder([msg.ids[ind], msg.ids[ind]], msg.ids[ind]+str(msg.types[ind]), msg.partition)

def create_diverged_message_memory(msg, ind):
    return memory_middleware.build_memory_messages_builder([msg.ids[ind], msg.ids[ind]]
                        , [QUERY_2_REVENUE, QUERY_2_QUANTITY], msg.ids[ind]+str(msg.types[ind]), msg.partition)



def configure_types_groupby(join_middleware, topk_middleware, topk_middleware_type = CSVHashedMessageBuilder):
    types_config = {}

    types_config[QUERY_2] = GroupbyTypeConfiguration(topk_middleware, 
            topk_middleware_type.creator_with_types(QUERY_2_REVENUE, QUERY_2_QUANTITY),
            in_fields = ["product_id", "month", "revenue", "quantity"], #EQUALS to out cols from select node main 
            grouping_conf = [["product_id", "month"], [
                [SUM_ACTION,"revenue"],
                #[COUNT_ACTION, "quantity_sold"],
                [SUM_ACTION,"quantity"],
            ]],
            out_conf={ROW_CONFIG_OUT_COLS: ["product_id", "month", "revenue", "quantity"]},
    )

    types_config[QUERY_3] = GroupbyTypeConfiguration(join_middleware, CSVHashedMessageBuilder.simple_creator(), 
            in_fields = ["store_id","mapped_semester","final_ammount"],  
            grouping_conf= [["store_id", "mapped_semester"], [
                [SUM_ACTION, "final_ammount", "tpv"],
                #[AVG_ACTION, "final_ammount", "avg"],
                #[COUNT_ACTION, "transaction_count"],
            ]],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","mapped_semester", "tpv"]},
    )

    types_config[QUERY_4] = GroupbyTypeConfiguration(topk_middleware, topk_middleware_type.simple_creator(),
            in_fields= ["store_id", "user_id"],
            grouping_conf= [["store_id", "user_id"], [
                [COUNT_ACTION,"purchase_count"] 
            ]],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","user_id", "purchase_count"]},
    )
    return types_config