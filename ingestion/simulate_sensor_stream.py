"""
Simulates an IoT sensor stream by reading train_FD001.txt row by row
and writing each reading as a JSON line to data/stream/incoming.jsonl.

Run in Terminal 1:
    python ingestion/simulate_sensor_stream.py
"""

import json
import os
import time
from datetime import datetime, timezone

DATASET_ID = "FD001"
INPUT_FILE = "data/raw/train_FD001.txt"
OUTPUT_FILE = "data/stream/incoming.jsonl"
EMIT_INTERVAL = 0.01  # seconds between rows
MAX_ROWS = 500        # stop after this many rows

COLUMNS = [
    "engine_id", "cycle",
    "op_setting_1", "op_setting_2", "op_setting_3",
    *[f"sensor_{i:02d}" for i in range(1, 22)],
]


def parse_row(line: str) -> dict:
    parts = line.strip().split()
    row = {}
    for col, val in zip(COLUMNS, parts):
        if col in ("engine_id", "cycle"):
            row[col] = int(val)
        else:
            row[col] = float(val)
    row["dataset_id"] = DATASET_ID
    row["emitted_at"] = datetime.now(timezone.utc).isoformat()
    return row


def read_rows(path: str) -> list[dict]:
    rows = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(parse_row(line))
    return rows


def emit(rows: list[dict], output_path: str, max_rows: int) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    rows = rows[:max_rows]
    print(f"[{DATASET_ID}] Emitting {len(rows)} rows (capped at {max_rows})")
    with open(output_path, "a") as out:
        for row in rows:
            out.write(json.dumps(row) + "\n")
            out.flush()
            print(
                f"[{DATASET_ID}] Engine {row['engine_id']} | "
                f"Cycle {row['cycle']} | emitted"
            )
            time.sleep(EMIT_INTERVAL)
    print(f"[{DATASET_ID}] Done — {len(rows)} rows emitted.")



if __name__ == "__main__":
    rows = read_rows(INPUT_FILE)
    print(f"[{DATASET_ID}] Loaded {len(rows):,} rows from {INPUT_FILE}")
    emit(rows, OUTPUT_FILE, MAX_ROWS)
