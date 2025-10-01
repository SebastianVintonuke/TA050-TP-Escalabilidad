
from middleware.routing import csv_message
from middleware.errors import * 
from middleware.routing.query_types import *

from .topk_type_config import * 
from .row_grouping import * 
"""
Transacciones (Id y monto) realizadas durante 2024 y 2025 entre las 06:00 AM y las
11:00 PM con monto total mayor o igual a 75.
2. Productos más vendidos (nombre y cant) y productos que más ganancias han generado
(nombre y monto), para cada mes en 2024 y 2025.
3. TPV (Total Payment Value) por cada semestre en 2024 y 2025, para cada sucursal, para
transacciones realizadas entre las 06:00 AM y las 11:00 PM.
4. Fecha de cumpleaños de los 3 clientes que han hecho más compras durante 2024 y
2025, para cada sucursal.           
"""

def configure_types_topk(join_middleware):
    types_config = {}
    
    #[KEEP_TOP_K, {'comp_key': "revenue", 'limit': 3}] # top k for products?
    types_config[QUERY_2_REVENUE] = TopKTypeConfiguration(join_middleware, 
            csv_message.csv_hashed_from_msg, 
            in_fields = ["product_id", "month", "revenue", "quantity_sold"],#["product_id", "month", "revenue"], 
            grouping_conf = [
                ["month"], [KEEP_TOP, {'comp_key': "revenue"}]
            ],
            out_conf={ROW_CONFIG_OUT_COLS: ["product_id", "month", "revenue"]},
    )

    types_config[QUERY_2_QUANTITY] = TopKTypeConfiguration(join_middleware, 
            csv_message.csv_hashed_from_msg, 
            in_fields = ["product_id", "month", "revenue", "quantity_sold"],#["product_id", "month", "quantity_sold"], 
            grouping_conf = [
                ["month"], [KEEP_TOP, {'comp_key': "quantity_sold"}]
            ],
            out_conf={ROW_CONFIG_OUT_COLS: ["product_id", "month", "quantity_sold"]},
    )

    types_config[QUERY_4] = TopKTypeConfiguration(join_middleware, csv_message.csv_hashed_from_msg,
            in_fields= ["store_id","user_id", "purchase_count"],
            grouping_conf = [
                ["store_id"], [KEEP_ASCDESC, {'comp_key': "purchase_count",'comp_key2': "user_id", 'limit': 3}] # Get higher in purchases and least user_id
            ],
            out_conf={ROW_CONFIG_OUT_COLS: ["store_id","user_id", "purchase_count"]},                
    )
    
    return types_config