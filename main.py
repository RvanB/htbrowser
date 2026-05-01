from datetime import datetime
from io import BufferedWriter
import duckdb
import requests
from dataclasses import dataclass
import json
from dateutil import parser
import gzip
import os

HATHI_FILE_BASE_URL = "https://www.hathitrust.org/files/hathifiles/"
HATHI_FILE_LIST = "hathi_file_list.json"
HATHI_FIELD_LIST = "hathi_field_list.txt"


@dataclass
class HathiFile:
    filename: str
    full: bool
    size: int
    created: datetime
    modified: datetime
    url: str


def get_hathifiles():
    response = requests.get(HATHI_FILE_BASE_URL + HATHI_FILE_LIST)

    hfs = []

    if response.ok:
        hfs_json = json.loads(response.content)
        for hf in hfs_json:
            hfs.append(
                HathiFile(
                    hf["filename"],
                    hf["full"],
                    hf["size"],
                    parser.parse(hf["created"]),
                    parser.parse(hf["modified"]),
                    hf["url"],
                )
            )
        return hfs
    else:
        raise


def prune_to_last_full_hathifile(hfs: list[HathiFile]):
    sorted_hfs = sorted(hfs, key=lambda hf: hf.created)

    last_full_idx = 0
    for i, hf in enumerate(sorted_hfs):
        if "full" in hf.filename:
            last_full_idx = i

    return sorted_hfs[last_full_idx:]


def create_collection(hfs: list[HathiFile], output_path: str):
    def download_stream(filename: str, fp: BufferedWriter):
        url = HATHI_FILE_BASE_URL + filename
        with requests.get(url, stream=True) as r:
            r.raise_for_status()

            if filename.endswith(".gz"):
                with gzip.GzipFile(fileobj=r.raw, mode="rb") as gz:
                    while True:
                        chunk = gz.read(8192)
                        if not chunk:
                            break
                        _ = fp.write(chunk)
            else:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        _ = fp.write(chunk)

    with open(output_path, mode="wb") as f:
        download_stream(HATHI_FIELD_LIST, f)
        for hf in hfs:
            download_stream(hf.filename, f)

def convert_csv_to_parquet(
    csv_path: str,
    outpath: str,
    row_group_size: int = 512_000,
):
    conn = duckdb.connect()

    print("Sorting into temp table...")
    _ = conn.execute(f"""
        CREATE TABLE sorted AS
        SELECT htid, title, author, TRY_CAST(rights_date_used AS INTEGER) AS rights_date_used, lang
        FROM read_csv('{csv_path}', ALL_VARCHAR=TRUE)
        WHERE access = 'allow'
        ORDER BY lower(coalesce(title, '')), lower(coalesce(author, ''))
    """)

    _ = conn.execute(f"""
        COPY (SELECT * FROM sorted)
        TO '{outpath}' (FORMAT 'PARQUET', CODEC 'ZSTD', ROW_GROUP_SIZE {row_group_size})
    """)


if __name__ == "__main__":
    print("Getting hathifiles")
    hfs = get_hathifiles()
    hfs = prune_to_last_full_hathifile(hfs)

    print("Creating collection")
    create_collection([hfs[0]], "collection.csv")

    print("Converting to parquet")
    convert_csv_to_parquet("collection.csv", "collection_temp.parquet")

    os.remove("collection_clean.csv")
    os.rename("collection_temp.parquet", "collection.parquet")

            
