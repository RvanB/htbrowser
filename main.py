from datetime import datetime
from io import BufferedWriter
import duckdb
import requests
from dataclasses import dataclass
import json
from dateutil import parser
import tempfile
import gzip
import io
import os
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from sentence_transformers import SentenceTransformer

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
    header = []
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


def convert_csv_to_parquet(csv_path: str, parquet_path):
    conn = duckdb.connect()
    _ = conn.execute(f"""
    COPY (SELECT * FROM read_csv('{csv_path}',AUTO_DETECT=TRUE))
    TO '{parquet_path}' (FORMAT 'PARQUET', CODEC 'ZSTD')
    """)


def embed_titles(parquet_path: str, batch_size: int = 10000):
    db_path = parquet_path + ".duckdb"
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    print("Using device" + str(model.device))
    conn = duckdb.connect(db_path)

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS data AS
        SELECT row_number() OVER () AS _row_id, *, NULL::FLOAT[] AS title_embedding
        FROM read_parquet('{parquet_path}')
    """)

    total = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
    done = conn.execute("SELECT COUNT(*) FROM data WHERE title_embedding IS NOT NULL").fetchone()[0]

    with tqdm(total=total, initial=done, unit="rows") as bar:
        while True:
            rows = conn.execute(f"""
                SELECT _row_id, title FROM data
                WHERE title_embedding IS NULL
                LIMIT {batch_size}
            """).fetchall()

            if not rows:
                break

            row_ids = [r[0] for r in rows]
            titles = [r[1] for r in rows]
            embeddings = model.encode(titles, show_progress_bar=False)

            conn.register("_batch", pa.table({
                "_row_id": pa.array(row_ids, type=pa.int64()),
                "title_embedding": pa.array(embeddings.tolist(), type=pa.list_(pa.float32())),
            }))
            conn.execute("""
                UPDATE data SET title_embedding = _batch.title_embedding
                FROM _batch WHERE data._row_id = _batch._row_id
            """)
            conn.unregister("_batch")
            bar.update(len(rows))

    print("Exporting to parquet...")
    tmp_path = parquet_path + ".tmp"
    conn.execute(f"""
        COPY (SELECT * EXCLUDE (_row_id) FROM data)
        TO '{tmp_path}' (FORMAT 'PARQUET', CODEC 'ZSTD')
    """)
    conn.close()
    os.replace(tmp_path, parquet_path)
    os.remove(db_path)


if __name__ == "__main__":
    if not os.path.exists("collection.csv"):
        print("Getting hathifiles")
        hfs = get_hathifiles()
        hfs = prune_to_last_full_hathifile(hfs)

        print("Creating collection")
        create_collection([hfs[0]], "collection.csv")

    if not os.path.exists("collection.parquet"):
        print("Converting to parquet")
        convert_csv_to_parquet("collection.csv", "collection.parquet")

    print("Embedding titles")
    embed_titles("collection.parquet")

            
