---
title: Metrics
nav_order: 9
---

# Metrics

Cobble Flink registers metrics automatically for state backends, sources,
lookups, and sink writers. No additional option is required. All metrics are
registered under the corresponding Flink operator metric group. This means
they carry Flink's normal job, task, operator, subtask, and attempt scope.
Cobble-specific metrics use the `cobble.` prefix in that same group.

Metrics belong to one subtask attempt. Counters start again when Flink creates a
new attempt after a restart or rescale. Aggregate across subtasks in your metric
reporter when you need job-level values.

## Storage And State Backend

The state backend and sink writers expose the following storage metrics:

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `cobble.compactionsTotal` | Counter | none | Successfully completed compactions. Remote compaction is counted after the writer applies the result. |
| `cobble.compactionReadBytesTotal` | Counter | none | Cumulative input bytes attributed to compaction. |
| `cobble.compactionWriteBytesTotal` | Counter | none | Cumulative output bytes produced by compaction. |
| `cobble.memtableFlushesTotal` | Counter | none | Successfully completed memtable flushes. |
| `cobble.memtableFlushBytesTotal` | Counter | none | Cumulative output bytes produced by memtable flushes. |
| `cobble.writeStallWaitsTotal` | Counter | none | Times a writer waited for write-stall pressure to clear. |
| `cobble.blockCacheHitsTotal` | Counter | `file`, `kind` | Successful SST block-cache lookups. `file=sst`; `kind=data\|index\|filter`. |
| `cobble.blockCacheMissesTotal` | Counter | `file`, `kind` | SST block-cache lookups that required a storage read. `file=sst`; `kind=data\|index\|filter`. |
| `cobble.blockCacheUsageBytes` | Gauge | `kind` | Current in-memory block-cache usage for `data`, `index`, `filter`, or `parquet_data`. The hybrid-cache disk tier is not included. |
| `cobble.sstBlockCompressionRatio` | Histogram | `compression` | Encoded SST block bytes divided by uncompressed bytes for `none` or `lz4`. |
| `cobble.dataFilesTracked` | Gauge | none | Data files currently tracked by Cobble. |
| `cobble.metadataFilesTracked` | Gauge | none | Metadata files currently tracked by Cobble. |
| `cobble.storageFileBytes` | Gauge | `volume` | Current tracked data and metadata bytes on a configured volume. `volume` is its zero-based position in the volume configuration. |
| `cobble.offloadJobsScheduledTotal` | Counter | none | Offload jobs accepted for background execution. |
| `cobble.offloadJobsCompletedTotal` | Counter | none | Offload jobs completed by copying or promoting a replica. |
| `cobble.offloadJobsFailedTotal` | Counter | none | Offload jobs that failed. |
| `cobble.offloadJobsNoopTotal` | Counter | none | Offload jobs that completed without moving a file. |
| `cobble.offloadBytesMovedTotal` | Counter | none | Cumulative bytes copied by completed offload jobs. |
| `cobble.offloadPromotionsTotal` | Counter | none | Offloads that promoted an existing replica instead of copying it. |

Remote compaction failures and skipped attempts do not change the compaction
counters. A successful local fallback is counted as a local compaction. Storage
metrics refresh approximately every five seconds, and a metric may be absent
until its associated feature has been used.

The compression histogram provides count, mean, minimum, and maximum. Quantiles
and standard deviation are unavailable.

## Source Scan

Source readers expose these standard Flink source metrics:

| Metric | Type | Meaning |
| --- | --- | --- |
| `numRecordsIn` | Counter | SQL rows emitted by the source reader. |
| `numBytesIn` | Counter | Raw key and column bytes fetched from Cobble. |
| `numRecordsInErrors` | Counter | Rows that failed during decoding or emission. |

`numRecordsIn` increases once per emitted SQL row. `numBytesIn` counts each
fetched entry once: its raw key plus all non-null raw columns. One ListState
entry can emit multiple rows, but its bytes are counted once.

The following Cobble counters use the same per-entry boundary:

| Metric | Meaning |
| --- | --- |
| `cobble.nativeEntriesReadTotal` | Entries fetched from Cobble. |
| `cobble.nativeBytesReadTotal` | Raw key and column bytes fetched from Cobble. |

## Lookup

Every lookup request has one hit, miss, or error outcome. Bytes are counted only
for hits and include the encoded key plus non-null returned columns. Missing
snapshots, keys, and state column families are misses.

| Metric | Type | Meaning |
| --- | --- | --- |
| `cobble.lookupRequestsTotal` | Counter | Lookup requests. |
| `cobble.lookupHitsTotal` | Counter | Requests that returned a row. |
| `cobble.lookupMissesTotal` | Counter | Requests with no row. |
| `cobble.lookupErrorsTotal` | Counter | Requests that failed. |
| `cobble.lookupBytesReadTotal` | Counter | Raw key and column bytes returned by successful lookups. |

## Sink

Sink writers expose these standard Flink sink metrics:

| Metric | Type | Meaning |
| --- | --- | --- |
| `numRecordsSend` | Counter | Successful Cobble mutations sent by the sink writer. |
| `numBytesSend` | Counter | Encoded key and value bytes written by successful mutations. |
| `numRecordsSendErrors` | Counter | Mutations that failed before completing successfully. |
| `numRecordsOut` | Counter | Flink operator I/O alias of `numRecordsSend`. |
| `numBytesOut` | Counter | Flink operator I/O alias of `numBytesSend`. |

Successful `INSERT`, `UPDATE_AFTER`, and `DELETE` mutations count one record;
`UPDATE_BEFORE` is ignored. Upsert bytes include the encoded key and non-null
values. Delete bytes include the encoded key. Encoding, ownership, unsupported
row-kind, and database mutation failures count one send error.

Sink writers also expose the [storage metrics](#storage-and-state-backend) for
their Cobble database.

## Reporter Example

Configure a Flink metric reporter normally. For example, Prometheus can be
enabled in `flink-conf.yaml`:

```yaml
metrics.reporters: prom
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prom.port: 9249-9250
```

Reporter identifiers also include the task, operator, subtask, attempt, and the
bounded labels shown above.
