import os
from io import StringIO, BytesIO

# import xml.etree.ElementTree as ET
from lxml import etree as ET
from json import JSONDecodeError
from typing import Iterable, Any
from multiprocessing import Pool, Manager
import sys
from xml.sax.handler import ContentHandler
from xml.sax import parseString

from data_pipeline.jsonl_util import read_jsonl
from data_pipeline.models import Datapoint
from data_pipeline.parsing_util import parse_arso_datetime, float_or_none
from data_pipeline.paths_util import get_data_dir


class MetDataXmlHandler(ContentHandler):
    def __init__(self):
        self.datapoints = []
        self.currentDatapoint = {}
        self.currentElement = ""
        self.currentCharacters = ""

    def startElement(self, name, attrs):
        self.currentElement = name
        self.currentCharacters = ""
        if name == "metData":
            self.currentDatapoint = {}

    def characters(self, content):
        self.currentCharacters += content

    def endElement(self, name):
        if name == "domain_meteosiId":
            self.currentDatapoint["station_arso_code"] = self.currentCharacters.strip("_")
        elif name == "sunrise":
            self.currentDatapoint["sunrise"] = parse_arso_datetime(self.currentCharacters)
        elif name == "sunset":
            self.currentDatapoint["sunset"] = parse_arso_datetime(self.currentCharacters)
        elif name == "validStart":
            self.currentDatapoint["interval_start"] = parse_arso_datetime(self.currentCharacters)
        elif name == "validEnd":
            self.currentDatapoint["interval_end"] = parse_arso_datetime(self.currentCharacters)
        elif name == "td":
            self.currentDatapoint["temperature_dew_point"] = float_or_none(self.currentCharacters)
        elif name == "tavg":
            self.currentDatapoint["temperature_air_avg"] = float_or_none(self.currentCharacters)
        elif name =="tx":
            self.currentDatapoint["temperature_air_max"] = float_or_none(self.currentCharacters)
        elif name == "tn":
            self.currentDatapoint["temperature_air_min"] = float_or_none(self.currentCharacters)
        elif name =="rhavg":
            self.currentDatapoint["humidity_relative_avg"] = float_or_none(self.currentCharacters)
        elif name =="ddavg_val":
            self.currentDatapoint["wind_direction_avg"] = float_or_none(self.currentCharacters)
        elif name =="ddmax_val":
            self.currentDatapoint["wind_direction_max_gust"] = float_or_none(self.currentCharacters)
        elif name =="ffavg_val":
            self.currentDatapoint["wind_speed_avg"] = float_or_none(self.currentCharacters)
        elif name =="ffmax_val":
            self.currentDatapoint["wind_speed_max"] = float_or_none(self.currentCharacters)
        elif name =="mslavg":
            self.currentDatapoint["pressure_mean_sea_level_avg"] = float_or_none(self.currentCharacters)
        elif name =="pavg":
            self.currentDatapoint["pressure_surface_level_avg"] = float_or_none(self.currentCharacters)
        elif name=="rr_val":
            self.currentDatapoint["precipitation_sum_10min"] = float_or_none(self.currentCharacters)
        elif name=="tp_1h_acc":
            self.currentDatapoint["precipitation_sum_1h"] = float_or_none(self.currentCharacters)
        elif name=="tp_24h_acc":
            self.currentDatapoint["precipitation_sum_24h"] = float_or_none(self.currentCharacters)
        elif name=="snow":
            self.currentDatapoint["snow_cover_height"] = float_or_none(self.currentCharacters)
        elif name=="gSunRadavg":
            self.currentDatapoint["sun_radiation_global_avg"] = float_or_none(self.currentCharacters)
        elif name=="diffSunRadavg":
            self.currentDatapoint["sun_radiation_diffuse_avg"] = float_or_none(self.currentCharacters)
        elif name=="vis_val":
            self.currentDatapoint["visibility"] = float_or_none(self.currentCharacters)
        elif name == "metData":
            self.datapoints.append(self.currentDatapoint)
            # self.datapoints.append(Datapoint.model_validate(self.currentDatapoint))


def xml_to_datapoints_sax(xml: str) -> Iterable[dict]:
    handler = MetDataXmlHandler()
    parseString(xml, handler)
    return handler.datapoints

def xml_to_datapoints(xml: str) -> Iterable[dict]:
    for action, met_data in ET.iterparse(BytesIO(xml.encode('utf-8')), events=('end',), tag='metData'):
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
        ).model_dump()


def xml_to_datapoints_with_clearing(xml: str) -> Iterable[dict]:
    context = iter(ET.iterparse(BytesIO(xml.encode('utf-8')), events=('start', 'end')))
    _, root = next(context)  # get root element

    for action, met_data in context:
        if action == "end" and met_data.tag == "metData":
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
            ).model_dump()
            met_data.clear()
            root.clear()


def upsert_datapoint(datapoint: dict, map):
    key = str((datapoint['station_arso_code'], datapoint['interval_start'], datapoint['interval_end']))
    map[key] = datapoint


def get_input_files_list(data_dir: str) -> list:
    meteo_data_archive_paths = [os.path.join(data_dir, filename) for filename in os.listdir(data_dir) if
                                (filename.endswith(".json") or filename.endswith(".json.gz")) and
                                filename.startswith("meteo_data_archive_")]
    meteo_data_archive_paths.sort()
    return meteo_data_archive_paths


def datapoints_in_file(file_path: str) -> Iterable[dict[str, Any]]:
    # print("Reading", file_path)
    try:
        data = read_jsonl(file_path)
        dps = [datapoint for row in data for datapoint in xml_to_datapoints_with_clearing(row['xml'])]
        # print("Loaded datapoints: ", len(dps))
        # print("Size: ", sys.getsizeof(dps))
        return dps
    except JSONDecodeError as e:
        print(f"Failed to read {file_path}: {e}")
    except EOFError as e:
        print(f"Failed to read {file_path}: {e}")

    return []


def upsert_datapoints_in_file(file_path: str, map):
    print("Reading", file_path)
    try:
        for datapoint in datapoints_in_file(file_path):
            upsert_datapoint(datapoint, map)
    except JSONDecodeError as e:
        print(f"Failed to read {file_path}: {e}")
    except EOFError as e:
        print(f"Failed to read {file_path}: {e}")
    print("Done loading", file_path)


#
def main():
    data_dir = get_data_dir()
    meteo_data_archive_paths = get_input_files_list(data_dir)

    d = dict()
    for fn in meteo_data_archive_paths:
        dps = datapoints_in_file(fn)
        for dp in dps:
            upsert_datapoint(dp, d)
        print("Count: ", len(d))
        if len(d) > 50000:
            break


def main_multiprocessing():
    data_dir = get_data_dir()
    meteo_data_archive_paths = get_input_files_list(data_dir)

    d = dict()
    counter = 0
    with Pool(processes=12, maxtasksperchild=1) as p:
        dps = p.imap(datapoints_in_file, meteo_data_archive_paths)
        for dpl in dps:
            for dp in dpl:
                counter += 1
                # upsert_datapoint(dp, d)
            print("Count: ", counter)


def main_multiprocessing_and_manager():
    data_dir = get_data_dir()
    meteo_data_archive_paths = get_input_files_list(data_dir)

    with Manager() as m:
        d = m.dict()
        with Pool(processes=12, maxtasksperchild=1) as p:
            p.starmap(upsert_datapoints_in_file, [(fn, d) for fn in meteo_data_archive_paths])

    print("Dict size: ", sys.getsizeof(d))
    print("Count: ", len(d))


if __name__ == "__main__":
    # main()
    main_multiprocessing()
