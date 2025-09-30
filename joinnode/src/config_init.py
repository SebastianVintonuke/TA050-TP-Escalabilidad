
from middleware.errors import *
from middleware.routing import csv_message
from middleware.routing.query_types import *

from src.join_type_config import *
from common.config.row_joining import *

def add_joinnode_config(types_expander, result_middleware):
    # Basic filter description
    """
        QUERY 2
            2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado
            (nombre y monto), para cada mes en 2024 y 2025.     
    """
    # Config for revenue on query2
    types_expander.add_configuration_to_many(
        JoinTypeConfiguration(result_middleware,csv_message.csv_hashed_from_msg,
            left_type= QUERY_PRODUCT_NAMES, #
            join_id = QUERY_2_REVENUE, 
            in_fields_left=["product_id","product_name"],  # ..product names
            in_fields_right=["top_product_id", "month", "revenue"],
            join_conf=[INNER_ON_EQ, {"col_left":"product_id", "col_right":"top_product_id"}],
            out_cols= ["product_id","product_name","month","revenue",]
        ),
        # Add the config to all these types... but.. internal join will separate/group content as needed
        QUERY_2_REVENUE,
        QUERY_PRODUCT_NAMES,
    )


    #Config for quantity on query2
    types_expander.add_configuration_to_many(
        JoinTypeConfiguration(result_middleware,csv_message.csv_hashed_from_msg,
            left_type= QUERY_PRODUCT_NAMES, 
            join_id=QUERY_2_QUANTITY,
            in_fields_left=["product_id","product_name"],  # ..product names
            in_fields_right=["top_product_id", "month", "quantity_sold"],
            join_conf=[INNER_ON_EQ, {"col_left":"product_id", "col_right":"top_product_id"}],
            out_cols= ["product_id","product_name","month","quantity_sold",]
        ),
        # Add the config to all these types... but.. internal join will separate/group content as needed
        QUERY_2_QUANTITY,
        QUERY_PRODUCT_NAMES,
    )

    """
        QUERY 3
            3. TPV (Total Payment Value) por cada semestre en 2024 y 2025, para cada sucursal, para
            transacciones realizadas entre las 06:00 AM y las 11:00 PM.  
    """    
    types_expander.add_configuration_to_many(
        JoinTypeConfiguration(result_middleware,csv_message.csv_hashed_from_msg,
            left_type= QUERY_STORE_NAMES, 
            join_id=QUERY_3,
            in_fields_left=["store_id","store_name"],  # ..store names
            in_fields_right=["store_id","mapped_semester", "tpv"],
            join_conf=[INNER_ON_EQ, {"col_left":"store_id", "col_right":"store_id"}],
            out_cols= ["store_id","store_name","mapped_semester","tpv",]
        ),
        # Add the config to all these types... but.. internal join will separate/group content as needed
        QUERY_3,
        QUERY_STORE_NAMES,
    )

    """
        QUERY 4
            4. Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y
            2025, para cada sucursal.
    """
    types_expander.add_configuration_to_many(
        JoinTypeConfiguration(result_middleware,csv_message.csv_hashed_from_msg,
            left_type= QUERY_USERS, 
            join_id=QUERY_4,
            in_fields_left=["user_id","birthday"],  # .. users birthday
            in_fields_right=["store_id","top_user_id", "purchase_count"],
            join_conf=[INNER_ON_EQ, {"col_left":"user_id", "col_right":"top_user_id"}],
            out_cols= ["store_id","user_id","birthday","purchase_count",]
        ),
        # Add the config to all these types... but.. internal join will separate/group content as needed
        QUERY_4,
        QUERY_USERS,
    )