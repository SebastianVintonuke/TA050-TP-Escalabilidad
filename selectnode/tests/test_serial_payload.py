import unittest

from middleware.routing.csv_message import *
from middleware.routing.header_fields import *

from common.config.row_filtering import *


def map_dict_to_vect(row):
    return [row["year"], row["hour"], row["sum"]]


def map_vect_to_dict(row):
    return {"year": int(row[0]), "hour": int(row[1]), "sum": int(row[2])}


class TestSerialPayload(unittest.TestCase):

    def test_serial_deserial_csv_message(self):
        msg_build = CSVMessageBuilder(BaseHeaders.default())

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 942},
            {"year": 2027, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 24, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 55},
        ]

        for itm in rows_pass:
            msg_build.add_row(map_dict_to_vect(itm))

        payload = msg_build.serialize_payload()
        res_msg = CSVMessage(payload)
        res = [itm for itm in res_msg.map_stream_rows(map_vect_to_dict)]

        self.assertTrue(len(rows_pass) == len(res))

        for i in range(len(rows_pass)):
            self.assertTrue(rows_pass[i] == res[i])

    def test_serial_deserial_hashed_csv_message(self):
        msg_build = CSVHashedMessageBuilder(BaseHeaders.default(), "KEY_HASH_1")

        rows_pass = [
            {"year": 2024, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 23, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 942},
            {"year": 2027, "hour": 7, "sum": 88},
            {"year": 2025, "hour": 24, "sum": 942},
            {"year": 2024, "hour": 6, "sum": 55},
        ]

        for itm in rows_pass:
            msg_build.add_row(map_dict_to_vect(itm))

        payload = msg_build.serialize_payload()
        res_msg = CSVMessage(payload)
        res = [itm for itm in res_msg.map_stream_rows(map_vect_to_dict)]

        self.assertTrue(len(rows_pass) == len(res))

        for i in range(len(rows_pass)):
            self.assertTrue(rows_pass[i] == res[i])
