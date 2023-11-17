import datetime
from typing import Optional

from pydantic import BaseModel


class Point(BaseModel):
    lon: float
    lat: float


class Station(BaseModel):
    arso_code: str
    coordinates: Point
    altitude: float
    name: str
    name_short: str
    name_long: str


class Datapoint(BaseModel):
    station_arso_code: str
    sunrise: datetime.datetime
    sunset: datetime.datetime
    interval_start: datetime.datetime  # from XML <validStart>
    interval_end: datetime.datetime  # from XML <validEnd>
    temperature_dew_point: Optional[float]  # from XML <td>
    temperature_air_avg: Optional[float]  # from XML <tavg>
    temperature_air_max: Optional[float]  # from XML <tx>
    temperature_air_min: Optional[float]  # from XML <tn>
    humidity_relative_avg: Optional[float]  # from XML <rhavg>
    wind_direction_avg: Optional[float]  # from XML <ddavg_val>
    wind_direction_max_gust: Optional[float]  # from XML <ddmax_val>
    wind_speed_avg: Optional[float]  # from XML <ffavg_val>
    wind_speed_max: Optional[float]  # from XML <ffmax_val>
    pressure_mean_sea_level_avg: Optional[float]  # from XML <mslavg>
    pressure_surface_level_avg: Optional[float]  # from XML <pavg>
    precipitation_sum_10min: Optional[float]  # from XML <rr_val>
    precipitation_sum_1h: Optional[float]  # from XML <tp_1h_acc>
    precipitation_sum_24h: Optional[float]  # from XML <tp_24h_acc>
    snow_cover_height: Optional[float]  # from XML <snow>
    sun_radiation_global_avg: Optional[float]  # from XML <gSunRadavg>
    sun_radiation_diffuse_avg: Optional[float]  # from XML <diffSunRadavg>
    visibility: Optional[float]  # from XML <vis_val>

    # TODO: Maybe add ground temperatures, cloud layers
