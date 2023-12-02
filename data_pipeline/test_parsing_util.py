import unittest
from datetime import datetime, timezone, timedelta

from data_pipeline.parsing_util import parse_arso_datetime


class TestParsingUtil(unittest.TestCase):
    def test_parse_arso_datetime(self):
        self.assertEqual(
            datetime(2023, 11, 10, 16, 40, tzinfo=timezone(timedelta(hours=1))),
            parse_arso_datetime("10.11.2023 16:40 CET"),
        )

        self.assertEqual(
            datetime(2023, 9, 3, 13, 30, tzinfo=timezone(timedelta(hours=2))),
            parse_arso_datetime("3.9.2023 13:30 CEST"),
        )

        self.assertEqual(
            datetime(2023, 9, 3, 13, 30, tzinfo=timezone(timedelta(hours=0))),
            parse_arso_datetime("3.9.2023 13:30 UTC"),
        )
