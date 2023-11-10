# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from dataclasses import dataclass


@dataclass
class Point:
    lon: float
    lat: float


@dataclass
class Station:
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
class StationArchiveXml:
    """Unparsed station archive XML"""

    meteosiId: str
    """Alphanumeric identifier of the station."""

    xml: str
    """XML data"""
