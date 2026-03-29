# Streaming Ingestion

Simulates MQTT-style IoT sensor data flowing into the Bronze layer, using only the filesystem as a transport layer.

## How it works

```
simulate_sensor_stream.py          ingest_to_bronze.py
        |                                  |
reads train_FD001.txt        watches data/stream/incoming.jsonl
        |                                  |
emits one row at a time  ──>  reads new lines since last poll
  as JSON Lines                            |
  every 0.5s                  writes each batch to Parquet
                               data/bronze/stream/*.parquet
```

`simulate_sensor_stream.py` acts as the sensor gateway — equivalent to an MQTT broker publishing readings. `ingest_to_bronze.py` acts as the stream consumer — equivalent to a Kafka consumer writing to the Bronze layer.

## Running

Open two terminals from the project root.

**Terminal 1 — start the emitter:**
```bash
python ingestion/simulate_sensor_stream.py
```

**Terminal 2 — start the ingestor:**
```bash
python ingestion/ingest_to_bronze.py
```

Both scripts run indefinitely. The emitter loops back to the start of the dataset after all rows are emitted.

## Output

| Path | Description |
|------|-------------|
| `data/stream/incoming.jsonl` | Append-only JSON Lines file (the "wire") |
| `data/stream/.ingest_position` | Byte offset tracking last-read position |
| `data/bronze/stream/*.parquet` | Parquet batches with `load_timestamp` added |

## Relation to the main pipeline

The Parquet files written to `data/bronze/stream/` follow the same schema as `data/bronze/sensor_readings` produced by `sensor_pulse_pipeline.ipynb`. The Silver and Gold layers can be extended to union both sources:

```python
df_batch  = spark.read.parquet("data/bronze/sensor_readings")
df_stream = spark.read.parquet("data/bronze/stream")
df_bronze = df_batch.union(df_stream)
```

In production this pattern maps to: MQTT → Kafka → Spark Structured Streaming → Delta Lake Bronze.
