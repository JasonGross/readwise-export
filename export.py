import argparse
from contextlib import contextmanager
import itertools
import csv
import json
import os
import sys
import time
from typing import Iterable, Optional, TypeVar
from pathlib import Path
import re
import shelve

import requests
from dotenv import load_dotenv

T = TypeVar("T")

try:
    from tqdm.auto import tqdm
except ImportError:

    @contextmanager
    def tqdm(iterable: T, *args, **kwargs):
        yield iterable


def fetch_all_documents(token: str) -> Iterable[dict]:
    next_page_cursor = None
    cache_dir = ".cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, "responses_cache")

    with shelve.open(cache_file) as cache:
        while True:
            params = {}
            if next_page_cursor:
                params["pageCursor"] = next_page_cursor
            cache_key = f"{params}"
            if cache_key in cache:
                response_json = cache[cache_key]
            else:
                response = requests.get(
                    url="https://readwise.io/api/v3/list/",
                    params=params,
                    headers={"Authorization": f"Token {token}"},
                )
                response_json = response.json()
                if "results" not in response_json:
                    if "detail" in response_json:
                        waiting = re.match("'Request was throttled. Expected available in ([0-9]*) seconds.'", response_json["detail"])
                        if waiting:
                            wait_time = int(waiting.group(1))
                            print(f"Waiting for {wait_time} seconds")
                            time.sleep(wait_time)
                            continue
                    raise ValueError(f"Error fetching data: {response_json}")
                cache[cache_key] = response_json

            for item in response_json["results"]:
                yield item
            next_page_cursor = response_json.get("nextPageCursor")
            if not next_page_cursor:
                break


def append_to_file(
    data: Iterable[dict],
    file_path: str | Path,
    file_format: Optional[str],
    *,
    overwrite: bool,
    allow_duplicates: bool,
) -> None:
    if file_format is None:
        file_format = Path(file_path).suffix.lstrip(".")
    if file_format == "jsonl":
        mode = "w" if overwrite else "a"
        existing_data_lines = set()
        if not overwrite and os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                existing_data_lines = {line.strip() for line in f}

        with open(file_path, mode, encoding="utf-8") as f:
            for item in tqdm(data):
                item_str = json.dumps(item)
                if allow_duplicates or item_str.strip() not in existing_data_lines:
                    f.write(item_str + "\n")
    elif file_format == "csv":
        mode = "w" if overwrite else "a"
        existing_data = []
        if not overwrite and os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)

        with open(file_path, mode, newline="", encoding="utf-8") as f:
            first_items, data = itertools.tee(data, 2)
            first_item = next(iter(first_items))
            if first_item is None:
                return
            writer = csv.DictWriter(f, fieldnames=first_item.keys())
            if overwrite or not os.path.exists(file_path):
                writer.writeheader()
            for item in tqdm(data):
                if allow_duplicates or item not in existing_data:
                    writer.writerow(item)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def main(argv: Optional[list[str]] = None):
    load_dotenv()
    token = os.getenv("READWISE_ACCESS_TOKEN")
    if not token:
        raise ValueError("READWISE_ACCESS_TOKEN not found in .env file")

    parser = argparse.ArgumentParser(
        description="Export Readwise documents to JSONL or CSV."
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="The file path to export data to.",
    )
    parser.add_argument(
        "--format",
        choices=["jsonl", "csv"],
        default=None,
        help="The format to export data in.",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite the existing file."
    )
    parser.add_argument(
        "--allow-duplicates",
        action="store_true",
        help="Allow duplicate entries in the file.",
    )

    args = parser.parse_args(argv)

    if args.format is None and args.output is None:
        args.output = "readwise_export.csv"
    elif args.output is None:
        args.output = f"readwise_export.{args.format}"

    data = fetch_all_documents(token)
    append_to_file(
        data,
        args.output,
        args.format,
        overwrite=args.overwrite,
        allow_duplicates=args.allow_duplicates,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
