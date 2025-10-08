
from middleware.errors import *

from middleware.routing.query_types import *
from .select_type_config import *
from common.config.row_filtering import *
from common.config.row_mapping import *


from middleware.routing.csv_message import CSVMessageBuilder,CSVHashedMessageBuilder


SHARED_IN_FIELDS = [
    "transaction_id",
    "year",
    "store_id",
    "user_id",
    "month",
    "hour",
    "revenue",
]


def add_selectnode_config(types_expander, result_middleware, groupby_middleware):
    # Basic filter description
    """
        QUERY 1
        1. Transacciones (Id y monto) realizadas durante 2024 y 2025 entre las 06:00 AM y las
        11:00 PM con monto total mayor o igual a 75.        
    """
    types_expander.add_configuration_to_many(
        SelectTypeConfiguration(
            result_middleware,
            CSVMessageBuilder.creator_with_type(QUERY_1),
            in_fields=SHARED_IN_FIELDS,  # In order
            filters_conf=[
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
                ["revenue", GREATER_EQ_THAN_OP, [75]],
            ],
            out_conf={ROW_CONFIG_OUT_COLS: ["transaction_id", "revenue"]},
        ),
        ALL_FOR_TRANSACTIONS,
        QUERY_1,  # add to query 1 independently for testing and so on.
    )

    """
        QUERY 2
            2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado
            (nombre y monto), para cada mes en 2024 y 2025.     
    """

    types_expander.add_configurations(
        QUERY_2,
        SelectTypeConfiguration(
            groupby_middleware,
            CSVHashedMessageBuilder.creator_with_type(QUERY_2),
            in_fields=["product_id", "year", "month", "revenue"],  # In order
            filters_conf=[["year", EQUALS_ANY, ["2024", "2025"]]],
            out_conf={
                ROW_CONFIG_ACTIONS: [
                    [
                        MAP_MONTH,
                        {
                            "init_year": 2024,
                            "col_year": "year",
                            "col_month": "month",
                            "col_out": "month",
                        },
                    ]
                ],
                ROW_CONFIG_OUT_COLS: ["product_id", "month", "revenue"],
            },
        ),
    )

    """
        QUERY 3
            3. TPV (Total Payment Value) por cada semestre en 2024 y 2025, para cada sucursal, para
            transacciones realizadas entre las 06:00 AM y las 11:00 PM.  
    """
    types_expander.add_configuration_to_many(
        SelectTypeConfiguration(
            groupby_middleware,
            CSVHashedMessageBuilder.creator_with_type(QUERY_3),
            in_fields=SHARED_IN_FIELDS,  # In order
            filters_conf=[
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["hour", BETWEEN_THAN_OP, [6, 23]],
            ],
            out_conf={
                ROW_CONFIG_ACTIONS: [
                    [
                        MAP_SEMESTER,
                        {
                            "init_year": 2024,
                            "col_year": "year",
                            "col_month": "month",
                            "col_out": "mapped_semester",
                        },
                    ]
                ],
                ROW_CONFIG_OUT_COLS: [
                    "store_id",
                    "mapped_semester",
                    "revenue",
                ],
            },
        ),
        ALL_FOR_TRANSACTIONS,
        QUERY_3,
    )
    """
        QUERY 4
            4. Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y
            2025, para cada sucursal.
    """
    types_expander.add_configuration_to_many(
        SelectTypeConfiguration(
            groupby_middleware,
            CSVHashedMessageBuilder.creator_with_type(QUERY_4),
            in_fields=SHARED_IN_FIELDS,  # In order
            filters_conf=[
                ["year", EQUALS_ANY, ["2024", "2025"]],
                ["user_id", NOT_EQUALS, ["", str(None)]],
            ],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id", "user_id"]},
        ),
        ALL_FOR_TRANSACTIONS,
        QUERY_4
    )