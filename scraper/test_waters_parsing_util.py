import datetime
import unittest
from pathlib import Path

from scrapy import Selector

from .items import RiverDatapoint, BuoyDatapoint, BuoyDatapointV2
from .waters_parsing_util import (
    float_or_none,
    int_or_none,
    parse_datetime,
    is_river_data_table,
    is_buoy_data_table,
    parse_river_datapoint,
    parse_buoy_datapoint,
    is_buoy_data_table_v2,
    parse_buoy_datapoint_v2,
)


class ParsingUtilTestCase(unittest.TestCase):
    buoy_data_html: str
    buoy_data: Selector
    buoy_data_v2_html: str
    buoy_data_v2: Selector
    river_data_html: str
    river_data: Selector

    @classmethod
    def setUpClass(cls) -> None:
        examples_dir = Path(__file__).parent / "examples"

        cls.buoy_data_html = (examples_dir / "buoy_data.html").read_text()
        cls.buoy_data = Selector(text=cls.buoy_data_html)
        cls.buoy_data_v2_html = (examples_dir / "buoy_data_v2.html").read_text()
        cls.buoy_data_v2 = Selector(text=cls.buoy_data_v2_html)
        cls.river_data_html = (examples_dir / "river_data.html").read_text()
        cls.river_data = Selector(text=cls.river_data_html)

    def test_float_or_none(self):
        self.assertEqual(1.23, float_or_none("1.23"))
        self.assertEqual(None, float_or_none("-"))

    def test_int_or_none(self):
        self.assertEqual(123, int_or_none("123"))
        self.assertEqual(None, int_or_none("-"))

    def test_parse_datetime(self):
        self.assertEqual(
            datetime.datetime(2023, 8, 13, 20, 50), parse_datetime("13.08.2023 20:50")
        )

    def test_is_river_data_table(self):
        self.assertFalse(is_river_data_table(self.buoy_data.css("table.podatki")))
        self.assertTrue(is_river_data_table(self.river_data.css("table.podatki")))

    def test_is_buoy_data_table(self):
        self.assertTrue(is_buoy_data_table(self.buoy_data.css("table.podatki")))
        self.assertFalse(is_buoy_data_table(self.river_data.css("table.podatki")))

    def test_is_buoy_data_table_v2(self):
        self.assertTrue(is_buoy_data_table_v2(self.buoy_data_v2.css("table.podatki")))
        self.assertFalse(is_buoy_data_table_v2(self.river_data.css("table.podatki")))

    def test_parse_river_datapoint(self):
        rows = self.river_data.css("table.podatki > tbody > tr")

        self.assertEqual(
            parse_river_datapoint("Location", rows[0]),
            RiverDatapoint(
                timestamp=datetime.datetime(2023, 8, 14, 7, 10),
                location="Location",
                water_level=None,
                flow_rate=None,
                temperature=None,
            ),
        )

        self.assertEqual(
            parse_river_datapoint("Location", rows[100]),
            RiverDatapoint(
                timestamp=datetime.datetime(2023, 8, 13, 14, 30),
                location="Location",
                water_level=327,
                flow_rate=89.3,
                temperature=14.9,
            ),
        )

    def test_parse_buoy_datapoint(self):
        rows = self.buoy_data.css("table.podatki > tbody > tr")
        self.assertEqual(
            parse_buoy_datapoint("Location", rows[0]),
            BuoyDatapoint(
                timestamp=datetime.datetime(2023, 8, 14, 7, 0),
                location="Location",
                temperature=25.3,
                waves_height=0.13,
                waves_period=2,
                waves_direction=214,
                flow_rate=28,
                flow_direction=359,
                max_waves_height=0.21,
            ),
        )

    def test_parse_buoy_datapoint_v2(self):
        rows = self.buoy_data_v2.css("table.podatki > tbody > tr")
        self.assertEqual(
            parse_buoy_datapoint_v2("Location", rows[0]),
            BuoyDatapointV2(
                timestamp=datetime.datetime(2023, 8, 25, 10, 0),
                location="Location",
                temperature=27.3,
                flow_direction_depth_2m=14,
                flow_rate_depth_2m=9,
                flow_direction_depth_10m=350,
                flow_rate_depth_10m=9,
                waves_height=0.08,
                max_waves_height=0.00,
                waves_direction=332,
                waves_period=3,
            ),
        )


if __name__ == "__main__":
    unittest.main()
