
from middleware.routing import csv_message
from middleware.errors import * 
from middleware.routing.query_types import *

from .groupby_type_config import * 

def create_diverged_message(msg, ind):
    return csv_message.CSVHashedMessageBuilder([msg.ids[ind]], [QUERY_2_REVENUE, QUERY_2_QUANTITY], msg.ids[ind]+str(msg.types[ind]), msg.partition)

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

    types_config[QUERY_3] = GroupbyTypeConfiguration(join_middleware, csv_message.csv_msg_from_msg, 
            in_fields = ["store_id","mapped_semester","revenue"],  
            grouping_conf= [["store_id", "mapped_semester"], {
                "revenue": SUM_ACTION,
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","mapped_semester", "revenue"]},
    )

    types_config[QUERY_4] = GroupbyTypeConfiguration(topk_middleware, csv_message.csv_hashed_from_msg,
            in_fields= ["transaction_id", "store_id", "user_id"],
            grouping_conf= [["store_id", "user_id"], {
                "purchase_count": COUNT_ACTION
            }],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","user_id", "purchase_count"]},                
    )
    return types_config
