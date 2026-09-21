# hyperdata-node-sidecar

Tails the `hyperdata-node` (hl-visor) output tree and streams **new lines** to Kafka as they are appended — plus **seal events** when hour-files finalize. It is the bridge between the node's only interface (appended JSONL files) and the streaming platform.

```
hyperdata-node ──► ~/hl/data/{node_fills, node_order_statuses,
                              node_raw_book_diffs}_streaming/hourly/<date>/<hour>
                        │  continuous append (~15.6 blocks/s, ~20 MB/s total)
                        ▼
            hyperdata-node-sidecar (this service)
                        │
                        ├─► data-plane topics (per table, one message per line):
                        │     hyperliquid.book-diffs        key: coin
                        │     hyperliquid.order-statuses    key: coin
                        │     hyperliquid.fills             key: coin
                        │
                        └─► seal topic (one message per finalized hour-file):
                              hyperliquid.node-files        key: table|date|hour
                              {table, path, date, hour, block_range, size, sha256, finalized_at}
```

## Why a tailer, not a watcher

The node appends continuously to the current hour's file (per table) and rotates at the hour. Waiting for the seal to ingest would (a) add up to 1h latency and (b) force downstream into 52 GB per-file commit units. So the sidecar **tails growth**: latency becomes seconds, and downstream Iceberg commits are checkpoint-aligned over a continuous stream (128–512 MB files land naturally). File seals remain as a *low-rate auxiliary* topic: durable archival + reconciliation watermarks.

## Contract

### Discovery & startup reconciliation

- **Full scan on startup**: enumerate all existing files under `WATCH_DIR` for the three tables; for each, read the sidecar's offset state; resume from the last committed offset (see *State* below). Start-order independence: the sidecar may start before/after the node; before/after Flink.
- Steady-state: **polling growth loop** (default 250 ms) over the current hour's files per table. Polling, not inotify — inotify events are unreliable on bind mounts / shared volumes, and polling a file's `size()` is one stat call.

### Per-line streaming (data plane)

- On each poll: `read(new_bytes)` for each grown file → parse complete JSON lines (a trailing partial line is held back until its terminator arrives — **line boundary tracking is mandatory**, appended chunks rarely align with newlines).
- Publish one Kafka message per line to the table's topic, **keyed by `coin`** — per-coin partition ordering, so the future stateful book job gets all diffs for a coin in one partition, in order.
- Message value: the raw JSON line, unchanged (no re-serialization; schema evolution is downstream's problem — see ingestion README census).
- Batching within a poll cycle is a producer-side efficiency knob (linger/compression), never a wire-format decision: **one record = one message**.
- The current hour's file rotates at :00: the tailer closes its reader, picks up the new hour file, and emits the **seal event** (below) for the rotated one.

### Seal events (`hyperliquid.node-files`)

Emitted once per finalized hour-file per table (~3/day):

```json
{"table": "node_raw_book_diffs_streaming", "path": ".../hourly/20260610/13",
 "date": "20260610", "hour": "13", "block_range": [1030140001, 1030196121],
 "size": 15320775857, "sha256": "...", "finalized_at": "2026-06-10T14:00:00Z"}
```

- sha256 computed over the sealed file; `block_range` from first/last line of the file.
- Consumers: backup/archive trigger, reconciliation watermark ("everything ≤ this offset is durable"), file-level integrity for backfill dedup.

### State (offsets)

- Per-file tail offsets persisted to a local state store (v0: append-only JSON/log file; SQLite if it earns it) — `{path: {byte_offset, last_line_complete}}`.
- **Crash semantics**: on restart, resume from the persisted offset. Between offset-persist and Kafka-ack, at-least-once is the guarantee: lines may be re-published after a crash; downstream (Flink → Iceberg) is exactly-once via checkpointing + deterministic keys `(table, block_number, seq_in_block)`, so replays are absorbed.
- Offsets are persisted **after** Kafka producer ack (delivery order: read → publish → ack → persist offset).

### Backup sink (pluggable)

- Interface: `BackupSink.backup(local_path) -> remote_ref`. v0: local SSD copy (same volume, `backups/` prefix). GCS/S3 implementations later — no cloud SDKs in the MVP.
- Seals are the natural backup trigger (whole, immutable, checksummed files only).

### Delivery semantics

- **At-least-once** everywhere, **exactly-once effect** at the warehouse via downstream idempotency. Retries with producer `idempotence=true` + `acks=all`; no transactions needed v0.

### Observability

- Prometheus `/metrics`: `lines_published{table}`, `bytes_read{table}`, `publish_lag_seconds` (now − block_time), `tail_offset_lag_bytes{file}`, `seals_emitted`, `partial_line_held_bytes`, `state_persist_failures`.
- Structured logs: one line per seal + poll-level lag warnings (tail lag > threshold).

## Environment

| Var | Default | Description |
| :--- | :--- | :--- |
| `WATCH_DIR` | `/watch` | Node output root (the three `*_streaming` trees beneath) |
| `KAFKA_BOOTSTRAP` | `kafka:29092` | Broker(s) |
| `TOPIC_BOOK_DIFFS` / `TOPIC_ORDER_STATUSES` / `TOPIC_FILLS` / `TOPIC_SEALS` | `hyperliquid.book-diffs` / `.order-statuses` / `.fills` / `hyperliquid.node-files` | Data-plane + seal topics |
| `POLL_INTERVAL_MS` | `250` | Growth poll cadence |
| `STATE_PATH` | `/state/tail_offsets.json` | Offset store location |
| `BACKUP_SINK` | `local` | Backup implementation selector |
| `BACKUP_DIR` | `/backups` | v0 local sink target |

## Engineering notes

- Language: **Python** (file tailing + confluent-kafka; one process, asyncio or threaded per-table tailers).
- The sidecar is the **hot path**: ~20 MB/s, ~40k msg/s at Jun-10 rates — producer tuning (linger, zstd compression batch) matters; see ingestion README for sizing.
- Replay-agnostic: the replay tap produces *progressive appends* (see `hyperdata-node/README.md`), so the tailer sees the same growth pattern live produces.

Status: skeleton — contract above is the build spec. See ingestion layer [milestones](../README.md).