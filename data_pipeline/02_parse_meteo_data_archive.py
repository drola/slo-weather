import os
import sys

from data_pipeline import db
from data_pipeline.jsonl_util import read_jsonl
from data_pipeline.models import Station, Point
from data_pipeline.paths_util import get_data_dir


def main():
    data_dir = get_data_dir()

    meteo_data_archive_paths = [os.path.join(data_dir, filename) for filename in os.listdir(data_dir) if
                                filename.endswith(".json") and filename.startswith("meteo_data_archive_")]
    meteo_data_archive_paths.sort()

    for path in meteo_data_archive_paths:
        data = read_jsonl(path)
        print(data[0]['xml'])
        sys.exit()


if __name__ == "__main__":
    main()
