
from middleware.routing import csv_message
from middleware import memory_middleware
from middleware.errors import * 
from middleware.routing.query_types import *

from .groupby_type_config import * 
from .row_aggregate import * 

def create_diverged_message(msg, ind):
    return csv_message.CSVHashedMessageBuilder([msg.ids[ind], msg.ids[ind]], [QUERY_2_REVENUE, QUERY_2_QUANTITY], msg.ids[ind]+str(msg.types[ind]), msg.partition)

def create_diverged_message_memory(msg, ind):
    return memory_middleware.build_memory_messages_builder([msg.ids[ind], msg.ids[ind]]
                        , [QUERY_2_REVENUE, QUERY_2_QUANTITY], msg.ids[ind]+str(msg.types[ind]), msg.partition)



def configure_types_groupby(join_middleware, topk_middleware):
    types_config = {}

    types_config[QUERY_2] = GroupbyTypeConfiguration(topk_middleware, 
            create_diverged_message, 
            in_fields = ["product_id", "month", "revenue"], #EQUALS to out cols from select node main 
            grouping_conf = [["product_id", "month"], {
                "revenue": SUM_ACTION,
                "quantity_sold": COUNT_ACTION
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["product_id", "month", "revenue", "quantity_sold"]},
    )

    types_config[QUERY_3] = GroupbyTypeConfiguration(join_middleware, csv_message.csv_hashed_from_msg, 
            in_fields = ["store_id","mapped_semester","revenue"],  
            grouping_conf= [["store_id", "mapped_semester"], {
                "revenue": SUM_ACTION,
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","mapped_semester", "revenue"]},
    )

    types_config[QUERY_4] = GroupbyTypeConfiguration(topk_middleware, csv_message.csv_hashed_from_msg,
            in_fields= ["store_id", "user_id"],
            grouping_conf= [["store_id", "user_id"], {
                "purchase_count": COUNT_ACTION
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","user_id", "purchase_count"]},                
    )
    return types_config

def configure_types_groupby_topk_memory(join_middleware, topk_middleware):
    types_config = {}

    types_config[QUERY_2] = GroupbyTypeConfiguration(topk_middleware, 
            create_diverged_message_memory, 
            in_fields = ["product_id", "month", "revenue"], #EQUALS to out cols from select node main 
            grouping_conf = [["product_id", "month"], {
                "revenue": SUM_ACTION,
                "quantity_sold": COUNT_ACTION
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["product_id", "month", "revenue", "quantity_sold"]},
    )

    types_config[QUERY_3] = GroupbyTypeConfiguration(join_middleware, csv_message.csv_hashed_from_msg, 
            in_fields = ["store_id","mapped_semester","revenue"],  
            grouping_conf= [["store_id", "mapped_semester"], {
                "revenue": SUM_ACTION,
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","mapped_semester", "revenue"]},
    )

    types_config[QUERY_4] = GroupbyTypeConfiguration(topk_middleware, memory_middleware.memory_builder_from_msg,
            in_fields= ["store_id", "user_id"],
            grouping_conf= [["store_id", "user_id"], {
                "purchase_count": COUNT_ACTION
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","user_id", "purchase_count"]},                
    )
    return types_config
