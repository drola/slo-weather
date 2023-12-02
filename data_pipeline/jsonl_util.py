"""Reading and writing JSONL files."""

import gzip
import json
from typing import Any, List

from scrapy.utils.serialize import ScrapyJSONEncoder


def read_jsonl(path: str) -> List[Any]:
    """Reads a JSONL file into a list."""

    if path.endswith(".gz"):
        with gzip.open(path, mode="r") as fp:
            return [json.loads(line) for line in fp]
    else:
        with open(path, mode="r") as fp:
            return [json.loads(line) for line in fp]


def write_jsonl(path: str, data: List[Any]) -> None:
    """Writes a list to a JSONL file."""
    with open(path, mode="w") as fp:
        for line in data:
            fp.write(json.dumps(line, cls=ScrapyJSONEncoder) + "\n")
