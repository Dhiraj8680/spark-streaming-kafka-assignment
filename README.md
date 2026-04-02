# Spark Structured Streaming Take-Home Assignment

## Overview

This project implements a **Spark Structured Streaming pipeline**
to process order events from Kafka using **event-time semantics**.

The pipeline is designed to handle:

* out-of-order events
* duplicate events
* late-arriving data
* stateful aggregations
* checkpoint-based recovery

The solution computes:

* total order value per customer using tumbling windows
* cancelled order counts per window

Results are persisted to **Parquet** using partitioned storage
and checkpoint-based fault tolerance.

---

## How to Run

### 1. Start Kafka Services

Run the provided Docker services:

```bash id="3e4r1p"
docker compose up -d --build
```

This starts:

* Zookeeper
* Kafka broker
* Kafka producer
* Kafka consumer

---

### 2. Run the Spark Job

```bash id="8b8t5r"
python streaming_job.py
```

---

### 3. Stop Docker Services

After execution:

```bash id="fptxj9"
docker compose down -v
```

---

## High-Level Architecture

The streaming pipeline follows the below flow:

```text id="mly9kk"
Kafka Topic (order_events)
        ↓
Spark ReadStream
        ↓
JSON Parsing + Schema Validation
        ↓
Event-Time Conversion
        ↓
Watermark + Deduplication
        ↓
Stateful Window Aggregations
        ↓
Parquet Output Sink
        ↓
Checkpoint + Recovery
```

### Components

* **Kafka** → source event stream
* **Spark Structured Streaming** → real-time processing
* **Watermarking** → late event handling
* **Deduplication** → duplicate event protection
* **Parquet Sink** → durable storage
* **Checkpointing** → restart recovery

---

### Watermarking

The stream uses:

```python id="kv4g2q"
.withWatermark("event_timestamp", "10 minutes")
```

This allows late-arriving events to be processed
for up to 10 minutes.

---

## Key Design and Execution Decisions

### 1. Watermark Configuration

The stream uses:

```python id="89x4d0"
.withWatermark("event_timestamp", "10 minutes")
```

A 10-minute watermark was selected
to tolerate late-arriving and out-of-order events
while preventing unbounded state growth.

---

### 2. Trigger Interval Selection

The output streams use:

```python id="fce6jl"
.trigger(processingTime="30 seconds")
```

A 30-second trigger interval was selected to 
maintain near real-time processing while 
keeping resource usage minimal on local machine.

---

### 3. State Growth Control Strategy

State growth is controlled using:

* watermarking
* bounded event-time windows
* `dropDuplicates()`

This ensures Spark does not retain state indefinitely.

---

### 4. Shuffle Minimization

The Spark session uses:

```python id="o34vvn"
.config("spark.sql.shuffle.partitions", "4")
```

This minimizes unnecessary shuffle tasks
for local machine execution.

---

### 5. Partitioning Strategy

Output data is partitioned by:

```text id="7o6vs4"
event_date
```

This improves downstream reads
through partition pruning.

---

## Checkpointing and Recovery Approach

The pipeline uses Structured Streaming checkpointing
for fault tolerance and restart recovery.

Each output stream uses a dedicated checkpoint path:

```text id="kxg34a"
checkpoints/customer_order_value_checkpoint
checkpoints/cancelled_orders_checkpoint
```
This ensures the stream is **restartable without data loss**.

---

### Duplicate Message Safety

Duplicate Kafka messages are handled using:

```python id="4xht1h"
dropDuplicates([
    "order_id",
    "product_id",
    "event_type",
    "event_timestamp"
])
```

This protects the pipeline from duplicate retries.

---

### Recovery Validation

The streaming application was intentionally stopped
and restarted using the same checkpoint directories.

After restart, the state-store continued from previous
delta batch files (for example `319.delta` → `328.delta`),
confirming successful restart recovery
without reprocessing from scratch.

---

## Assumptions and Trade-offs

### Assumptions

* Kafka producer sends JSON messages
  matching the provided schema
* `event_time` is always present
  and parseable as timestamp
* duplicate events may occur
* events may arrive out of order

---

### Trade-offs

### 1. Watermark Duration

A 10-minute watermark was selected
to balance correctness and state-store memory usage.

Larger watermark:

* better late-event tolerance
* more state memory usage

Smaller watermark:

* faster output finalization
* risk of dropping late events

---

### 2. Local Machine Optimization

Shuffle partitions were reduced to `4`
to optimize execution on a local machine.


---

### 3. Notebook Validation Adjustments

For notebook validation, temporary
1-minute windows and watermark durations
were used to accelerate parquet file generation.

The final design logic remains based on
5-minute tumbling windows and
10-minute watermark configuration.

---

## Conclusion

This project successfully implements a robust
Spark Structured Streaming pipeline using Kafka
as the streaming source.

The solution demonstrates:

* event-time stream processing
* stateful deduplication
* late data handling with watermark
* window-based aggregations
* parquet-based durable output
* checkpoint-driven fault tolerance
* restart recovery validation


