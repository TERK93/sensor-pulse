"""
Watches data/stream/incoming.jsonl for new lines and writes each batch
to data/bronze/stream/ as Parquet (append-only).

Run in Terminal 2 (while simulate_sensor_stream.py is running):
    python ingestion/ingest_to_bronze.py
"""

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

STREAM_FILE = "data/stream/incoming.jsonl"
BRONZE_DIR = "data/bronze/stream"
POLL_INTERVAL = 1.0  # seconds between file checks
POSITION_FILE = "data/stream/.ingest_position"


def read_position() -> int:
    if os.path.exists(POSITION_FILE):
        with open(POSITION_FILE, "r") as f:
            return int(f.read().strip())
    return 0


def write_position(pos: int) -> None:
    os.makedirs(os.path.dirname(POSITION_FILE), exist_ok=True)
    with open(POSITION_FILE, "w") as f:
        f.write(str(pos))


def read_new_lines(path: str, position: int) -> tuple[list[dict], int]:
    if not os.path.exists(path):
        return [], position
    rows = []
    with open(path, "r") as f:
        f.seek(position)
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        new_position = f.tell()
    return rows, new_position


def write_parquet_batch(rows: list[dict], bronze_dir: str) -> None:
    os.makedirs(bronze_dir, exist_ok=True)
    load_ts = datetime.now(timezone.utc).isoformat()
    for row in rows:
        row["load_timestamp"] = load_ts

    df = pd.DataFrame(rows)

    # Cast types to match the Bronze schema used in the main pipeline
    int_cols = ["engine_id", "cycle"]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype("int32")

    float_cols = [c for c in df.columns if c not in int_cols + ["dataset_id", "emitted_at", "load_timestamp"]]
    for col in float_cols:
        df[col] = df[col].astype("float64")

    table = pa.Table.from_pandas(df, preserve_index=False)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    out_path = os.path.join(bronze_dir, f"batch_{timestamp}.parquet")
    pq.write_table(table, out_path)

    for row in rows:
        print(
            f"[BRONZE] 1 row ingested | "
            f"engine_id={row['engine_id']} | "
            f"cycle={row['cycle']}"
        )


def watch(stream_file: str, bronze_dir: str) -> None:
    position = read_position()
    print(f"[BRONZE] Watching {stream_file} from byte offset {position}")
    while True:
        rows, new_position = read_new_lines(stream_file, position)
        if rows:
            write_parquet_batch(rows, bronze_dir)
            position = new_position
            write_position(position)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    watch(STREAM_FILE, BRONZE_DIR)
