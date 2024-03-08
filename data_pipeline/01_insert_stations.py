import os

from data_pipeline import db
from data_pipeline.jsonl_util import read_jsonl
from data_pipeline.models import Station, Point
from data_pipeline.paths_util import get_data_dir


def main():
    data_dir = get_data_dir()

    stations_data_paths = [os.path.join(data_dir, filename) for filename in os.listdir(data_dir) if
                           (filename.endswith(".json") or filename.endswith(".json.gz")) and
                           filename.startswith("stations_")]
    stations_data_paths.sort()

    # We have to iterate through all files, because stations can appear and disappear
    # In stations_by_arso_code we'll keep the newest information for each station.
    stations_by_arso_code = {}
    for stations_data_path in stations_data_paths:
        print("Reading", stations_data_path)
        try:
            stations_raw = read_jsonl(stations_data_path)
            for station_raw in stations_raw:
                stations_by_arso_code[station_raw["meteosiId"]] = station_raw
        except Exception as e:
            print(f"Failed to read {stations_data_path}: {e}")

    with db.connect() as conn:
        with conn.cursor() as cur:
            for station_raw in stations_by_arso_code.values():
                station = Station(
                    arso_code=station_raw["meteosiId"],
                    coordinates=Point(**station_raw["coordinates"]),
                    altitude=station_raw["altitude"],
                    name=station_raw["title"],
                    name_short=station_raw["shortTitle"],
                    name_long=station_raw["longTitle"],
                )
                print(station)

                # upsert station
                cur.execute(
                    "INSERT INTO stations(arso_code, coordinates, altitude, name, name_short, name_long, latitude, longitude) "
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (arso_code) DO UPDATE SET "
                    "coordinates = EXCLUDED.coordinates, "
                    "altitude = EXCLUDED.altitude, "
                    "name = EXCLUDED.name, "
                    "name_short = EXCLUDED.name_short, "
                    "name_long = EXCLUDED.name_long, "
                    "latitude = EXCLUDED.latitude, "
                    "longitude = EXCLUDED.longitude "
                    "RETURNING id",
                    (
                        station.arso_code,
                        (station.coordinates.lon, station.coordinates.lat),
                        station.altitude,
                        station.name,
                        station.name_short,
                        station.name_long,
                        station.coordinates.lat,
                        station.coordinates.lon
                    ),
                )


if __name__ == "__main__":
    main()
