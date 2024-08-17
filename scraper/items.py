# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Point:
    lon: float
    lat: float


@dataclass
class WeatherStation:
    meteosiId: str
    """Alphanumeric identifier of the station."""

    countryIsoCode2: str
    """ISO 3166-1 alpha-2 country code."""

    coordinates: Point
    """Coordinates of the station."""

    altitude: float
    """Altitude of the station in meters."""

    title: str
    """Short location name of the station."""

    longTitle: str
    """Longer descriptive name of the station."""

    shortTitle: str
    """Shorter descriptive name of the station."""


@dataclass
class WeatherStationArchiveXml:
    """Unparsed station archive XML"""

    meteosiId: str
    """Alphanumeric identifier of the station."""

    xml: str
    """XML data"""

@dataclass
class RiverDatapoint:
    location: str
    timestamp: datetime
    water_level: Optional[int]
    flow_rate: Optional[float]
    temperature: Optional[float]


@dataclass
class BuoyDatapoint:
    location: str
    timestamp: datetime
    temperature: Optional[float]
    waves_height: Optional[float]
    waves_period: Optional[int]
    waves_direction: Optional[int]
    flow_rate: Optional[int]
    flow_direction: Optional[int]
    max_waves_height: Optional[float]


@dataclass
class BuoyDatapointV2:
    """Added in August 2023"""

    location: str
    timestamp: datetime
    temperature: Optional[float]

    flow_direction_depth_2m: Optional[int]
    flow_rate_depth_2m: Optional[int]
    flow_direction_depth_10m: Optional[int]
    flow_rate_depth_10m: Optional[int]

    waves_height: Optional[float]
    max_waves_height: Optional[float]
    waves_direction: Optional[int]
    waves_period: Optional[int]


@dataclass
class BuoyDatapointV3:
    location: str
    timestamp: datetime
    water_level: Optional[int]
    temperature: Optional[float]


@dataclass
class HtmlDatapoint:
    """When we weren't able to parse table into other data structures"""

    html: str
