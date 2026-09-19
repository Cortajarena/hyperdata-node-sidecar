# hyperliquid-node-sidecar

Watches the `hyperdata-node` (hl-visor) output tree and publishes a **notification** to Kafka for every new file landed.

```
hyperdata-node ──► ~/hl/data/{node_fills, node_order_statuses,
                              node_raw_book_diffs, ...}/hourly/<date>/<hour>
                        │
                        ▼
              hyperliquid-node-sidecar (this service)
                        │  watch (inotify / polling fallback)
                        ▼
              Kafka topic: hyperliquid.node-files
              {table, path, date, hour, block_range, size, checksum}
```

Design constraints (see [../README.md](../README.md) for full pipeline context):

- **Notifications only** — the payload is file metadata; data never flows through Kafka.
- **Replay-agnostic** — files copied by the replay script are indistinguishable from live ones; the sidecar publishes both.
- **At-least-once delivery** with idempotency keys (table + path + checksum); downstream Flink/Iceberg commits are exactly-once.
- Watched root, Kafka bootstrap, topic and consumer group are env-config (`WATCH_DIR`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC`, ...).

Status: skeleton. See ingestion layer [milestones](../README.md#milestones-v001).