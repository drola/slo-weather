import datetime
from typing import Optional

from scrapy import Selector
from scrapy.selector import SelectorList

from .items import (
    RiverDatapoint,
    BuoyDatapoint,
    BuoyDatapointV2,
    BuoyDatapointV3,
    HtmlDatapoint,
)


def float_or_none(s: str) -> Optional[float]:
    try:
        return float(s)
    except ValueError:
        return None


def int_or_none(s: str) -> Optional[float]:
    try:
        return int(s)
    except ValueError:
        return None


def parse_datetime(s: str) -> datetime.datetime:
    return datetime.datetime.strptime(
        s, "%d.%m.%Y %H:%M"
    )  # Example: "13.08.2023 17:00"


def is_river_data_table(e: SelectorList) -> bool:
    headers = e.css("thead > tr > th::text").getall()
    return headers == [
        "Datum",
        "Vodostaj [cm]",
        "Pretok [m³/s]",
        "Temperatura vode [°C]",
    ]


def is_buoy_data_table(e: SelectorList) -> bool:
    headers = e.css("thead > tr > th::text").getall()
    return headers == [
        "Datum",
        "Temperatura vode [°C]",
        "Višina valov [m]",
        "Vrednost periode valov [s]",
        "Smer valov [°]",
        "Hitrost morskega toka [cm/s]",
        "Smer morskega toka [°]",
        "Maksimalna višina valov [m]",
    ]


def is_buoy_data_table_v2(e: SelectorList) -> bool:
    headers = e.css("thead > tr > th::text").getall()
    return headers == [
        "Datum",
        "Temperatura vode [°C]",
        "Smer morskega toka 2m pod gladino [°]",
        "Hitrost morskega toka 2m pod glad. [cm/s]",
        "Smer morskega toka 10m pod gladino [°]",
        "Hitrost morskega toka 10m pod glad. [cm/s]",
        "Značilna višina valov [m]",
        "Maksimalna višina valov [m]",
        "Smer valovanja [°]",
        "Perioda valovanja [s]",
    ]


def is_buoy_data_table_v3(e: SelectorList) -> bool:
    headers = e.css("thead > tr > th::text").getall()
    return headers == ["Datum", "Vodostaj [cm]", "Temperatura vode [°C]"]


def parse_river_datapoint(location: str, e: Selector) -> RiverDatapoint:
    fields = e.css("td::text").getall()
    return RiverDatapoint(
        location=location,
        timestamp=parse_datetime(fields[0]),
        water_level=int_or_none(fields[1]),
        flow_rate=float_or_none(fields[2]),
        temperature=float_or_none(fields[3]),
    )


def parse_buoy_datapoint(location: str, e: Selector) -> BuoyDatapoint:
    fields = e.css("td::text").getall()
    return BuoyDatapoint(
        location=location,
        timestamp=parse_datetime(fields[0]),
        temperature=float_or_none(fields[1]),
        waves_height=float_or_none(fields[2]),
        waves_period=int_or_none(fields[3]),
        waves_direction=int_or_none(fields[4]),
        flow_rate=int_or_none(fields[5]),
        flow_direction=int_or_none(fields[6]),
        max_waves_height=float_or_none(fields[7]),
    )


def parse_buoy_datapoint_v2(location: str, e: Selector) -> BuoyDatapointV2:
    fields = e.css("td::text").getall()
    return BuoyDatapointV2(
        location=location,
        timestamp=parse_datetime(fields[0]),
        temperature=float_or_none(fields[1]),
        flow_direction_depth_2m=int_or_none(fields[2]),
        flow_rate_depth_2m=int_or_none(fields[3]),
        flow_direction_depth_10m=int_or_none(fields[4]),
        flow_rate_depth_10m=int_or_none(fields[5]),
        waves_height=float_or_none(fields[6]),
        max_waves_height=float_or_none(fields[7]),
        waves_direction=int_or_none(fields[8]),
        waves_period=int_or_none(fields[9]),
    )


def parse_buoy_datapoint_v3(location: str, e: Selector) -> BuoyDatapointV3:
    fields = e.css("td::text").getall()
    return BuoyDatapointV3(
        location=location,
        timestamp=parse_datetime(fields[0]),
        water_level=int_or_none(fields[1]),
        temperature=float_or_none(fields[2]),
    )


def parse_page_with_table(table: SelectorList, location: str, raw_html) -> list:
    rows = table.css("tbody > tr")

    if is_river_data_table(table):
        return [parse_river_datapoint(location, row) for row in rows]
    elif is_buoy_data_table(table):
        return [parse_buoy_datapoint(location, row) for row in rows]
    elif is_buoy_data_table_v2(table):
        return [parse_buoy_datapoint_v2(location, row) for row in rows]
    elif is_buoy_data_table_v3(table):
        return [parse_buoy_datapoint_v3(location, row) for row in rows]
    else:
        return [HtmlDatapoint(html=raw_html)]
