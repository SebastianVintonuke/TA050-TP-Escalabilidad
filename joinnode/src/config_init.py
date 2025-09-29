
from middleware.errors import *
from middleware.routing import csv_message
from middleware.routing.query_types import *
from src.select_type_config import *
from common.config.row_filtering import *
from common.config.row_mapping import *

SHARED_IN_FIELDS = [
    "transaction_id",
    "year",
    "store_id",
    "user_id",
    "month",
    "hour",
    "revenue",
]

def add_joinnode_config(types_expander, result_middleware):
    # Basic filter description
    """
        QUERY 2
            2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado
            (nombre y monto), para cada mes en 2024 y 2025.     
    """
    types_expander.add_configuration_to_many(
        JoinTypeConfiguration(result_middleware,csv_message.csv_hashed_from_msg,
            type_left= QUERY_PRODUCT_NAMES, #
            in_fields_left=["product_id","product_name"],  # ..product names
            in_fields_right=["top_product_id", "month", "quantity"], # Quantity might be revenue or quantity sold.
            join_conf=[
                [INNER_ON_EQ, {"left_col":"product_id", "right_col":"top_product_id"}]
            ],
            out_conf={ # Joined row has [... in left .. in right]
                ROW_CONFIG_OUT_COLS: [
                    "product_id",
                    "product_name",
                    "month",
                    "quantity",
                ],
            },
        ),
        # Add the config to all these types... but.. internal join will separate/group content as needed
        QUERY_2_REVENUE,
        QUERY_2_QUANTITY,
        QUERY_PRODUCT_NAMES,
    )