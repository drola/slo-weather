import os
import xml.etree.ElementTree as ET
from typing import Iterable

from data_pipeline import db
from data_pipeline.jsonl_util import read_jsonl
from data_pipeline.models import Datapoint
from data_pipeline.parsing_util import parse_arso_datetime, float_or_none
from data_pipeline.paths_util import get_data_dir


def xml_to_datapoints(xml: str) -> Iterable[Datapoint]:
    tree = ET.fromstring(xml)
    for met_data in tree.findall("metData"):
        yield Datapoint(
            station_arso_code=met_data.find("domain_meteosiId").text.strip("_"),
            sunrise=parse_arso_datetime(met_data.find("sunrise").text),
            sunset=parse_arso_datetime(met_data.find("sunset").text),
            interval_start=parse_arso_datetime(met_data.find("validStart").text),
            interval_end=parse_arso_datetime(met_data.find("validEnd").text),
            temperature_dew_point=float_or_none(met_data.find("td").text),
            temperature_air_avg=float_or_none(met_data.find("tavg").text),
            temperature_air_max=float_or_none(met_data.find("tx").text),
            temperature_air_min=float_or_none(met_data.find("tn").text),
            humidity_relative_avg=float_or_none(met_data.find("rhavg").text),
            wind_direction_avg=float_or_none(met_data.find("ddavg_val").text),
            wind_direction_max_gust=float_or_none(met_data.find("ddmax_val").text),
            wind_speed_avg=float_or_none(met_data.find("ffavg_val").text),
            wind_speed_max=float_or_none(met_data.find("ffmax_val").text),
            pressure_mean_sea_level_avg=float_or_none(met_data.find("mslavg").text),
            pressure_surface_level_avg=float_or_none(met_data.find("pavg").text),
            precipitation_sum_10min=float_or_none(met_data.find("rr_val").text),
            precipitation_sum_1h=float_or_none(met_data.find("tp_1h_acc").text),
            precipitation_sum_24h=float_or_none(met_data.find("tp_24h_acc").text),
            snow_cover_height=float_or_none(met_data.find("snow").text),
            sun_radiation_global_avg=float_or_none(met_data.find("gSunRadavg").text),
            sun_radiation_diffuse_avg=float_or_none(met_data.find("diffSunRadavg").text),
            visibility=float_or_none(met_data.find("vis_val").text)
        )


def upsert_datapoint(datapoint: Datapoint, cursor):
    cursor.execute("""
    INSERT INTO weather_datapoints (
        station_arso_code,
        sunrise,
        sunset,
        interval_start,
        interval_end,
        temperature_dew_point,
        temperature_air_avg,
        temperature_air_max,
        temperature_air_min,
        humidity_relative_avg,
        wind_direction_avg,
        wind_direction_max_gust,
        wind_speed_avg,
        wind_speed_max,
        pressure_mean_sea_level_avg,
        pressure_surface_level_avg,
        precipitation_sum_10min,
        precipitation_sum_1h,
        precipitation_sum_24h,
        snow_cover_height,
        sun_radiation_global_avg,
        sun_radiation_diffuse_avg,
        visibility
    ) VALUES (
        %(station_arso_code)s,
        %(sunrise)s,
        %(sunset)s,
        %(interval_start)s,
        %(interval_end)s,
        %(temperature_dew_point)s,
        %(temperature_air_avg)s,
        %(temperature_air_max)s,
        %(temperature_air_min)s,
        %(humidity_relative_avg)s,
        %(wind_direction_avg)s,
        %(wind_direction_max_gust)s,
        %(wind_speed_avg)s,
        %(wind_speed_max)s,
        %(pressure_mean_sea_level_avg)s,
        %(pressure_surface_level_avg)s,
        %(precipitation_sum_10min)s,
        %(precipitation_sum_1h)s,
        %(precipitation_sum_24h)s,
        %(snow_cover_height)s,
        %(sun_radiation_global_avg)s,
        %(sun_radiation_diffuse_avg)s,
        %(visibility)s
    ) ON CONFLICT (station_arso_code, interval_start, interval_end) DO UPDATE SET
        sunrise = excluded.sunrise,
        sunset = excluded.sunset,
        temperature_dew_point = excluded.temperature_dew_point,
        temperature_air_avg = excluded.temperature_air_avg,
        temperature_air_max = excluded.temperature_air_max,
        temperature_air_min = excluded.temperature_air_min,
        humidity_relative_avg = excluded.humidity_relative_avg,
        wind_direction_avg = excluded.wind_direction_avg,
        wind_direction_max_gust = excluded.wind_direction_max_gust,
        wind_speed_avg = excluded.wind_speed_avg,
        wind_speed_max = excluded.wind_speed_max,
        pressure_mean_sea_level_avg = excluded.pressure_mean_sea_level_avg,
        pressure_surface_level_avg = excluded.pressure_surface_level_avg,
        precipitation_sum_10min = excluded.precipitation_sum_10min,
        precipitation_sum_1h = excluded.precipitation_sum_1h,
        precipitation_sum_24h = excluded.precipitation_sum_24h,
        snow_cover_height = excluded.snow_cover_height,
        sun_radiation_global_avg = excluded.sun_radiation_global_avg,
        sun_radiation_diffuse_avg = excluded.sun_radiation_diffuse_avg,
        visibility = excluded.visibility
    """, datapoint.model_dump())


def datapoints_in_dir(data_dir: str) -> Iterable[Datapoint]:
    meteo_data_archive_paths = [os.path.join(data_dir, filename) for filename in os.listdir(data_dir) if
                                (filename.endswith(".json") or filename.endswith(".json.gz")) and
                                filename.startswith("meteo_data_archive_")]
    meteo_data_archive_paths.sort()

    for path in meteo_data_archive_paths:
        data = read_jsonl(path)
        for row in data:
            for datapoint in xml_to_datapoints(row['xml']):
                yield datapoint


def main():
    data_dir = get_data_dir()
    with db.connect() as conn:
        with conn.cursor() as cur:
            datapoint_counter = 0

            for datapoint in datapoints_in_dir(data_dir):
                upsert_datapoint(datapoint, cur)
                datapoint_counter += 1

                if datapoint_counter % 1000 == 0:
                    print(f"Inserted {datapoint_counter} datapoints")

            print(f"Inserted {datapoint_counter} datapoints")


if __name__ == "__main__":
    main()
