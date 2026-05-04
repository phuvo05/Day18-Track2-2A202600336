# Bonus Architecture — LLM Observability at 1 Billion Requests/Day

**Author:** Võ Thiên Phú | MSSV: 2A202600336
**Topic:** A — LLM observability at 1B requests/day
**Date:** 2026-05-04

---

## 1. Problem Statement

A foundation-model API team must log every LLM request and response at scale. The system processes **1 billion requests per day**, each record averaging **5 KB** (prompt + response + metadata). This yields **5 TB/day raw** — the equivalent of ingesting the entire Library of Congress every few hours.

Key constraints:

| Constraint | Target |
|---|---|
| Ingestion rate | 1 B req/day ≈ 57,870 req/sec sustained |
| Raw record size | ~5 KB / req |
| Per-tenant dashboards | Refreshed every **5 minutes** |
| Full prompt/response retention | **7 days** hot, then aggregates only |
| Year-long aggregates retention | 1 year |
| PII protection | Redacted before any human can read it |
| Storage budget | **≤ $5,000 / month** total |

The problem is hard because three axes pull in different directions simultaneously: **write throughput** (1B/day), **read latency** (5-min dashboards), and **cost** (tight FinOps ceiling). A naive approach — store everything in one big S3 bucket — either exceeds budget or collapses under query load.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INGESTION PATH (Bronze)                             │
│                                                                              │
│  ┌──────────┐    ┌───────────────┐    ┌──────────────────────────────────┐  │
│  │ LLM API  │───▶│ Kafka / Kinesis│───▶│ Stream Consumer (Flink/Spark)    │  │
│  │ Services │    │ (≥ 3 replicas) │    │ • PII tokenization (SHA-256)     │  │
│  └──────────┘    └───────────────┘    │ • Schema validation              │  │
│                                        │ • Partition: date/tenant_id     │  │
│                                        │ • Write to Bronze Delta Lake    │  │
│                                        └──────────┬───────────────────┘   │
│                                                   │                         │
│  Partition scheme: date=<YYYY-MM-DD>/tenant_id=<id>/batch_<seq>.parquet   │
│  Delta features used:                                                     │
│    - ACID writes (concurrent stream consumers)                            │
│    - Schema enforcement (drop malformed records, never corrupt)            │
│    - Deletion vectors (mark failed批次 as soft-deleted)                    │
│    - Column mapping (handle field renames across model upgrades)          │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SILVER TRANSFORMATION (CDF)                         │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ Delta CDF (Change Data Feed) → Debezium or Spark micro-batch          │  │
│  │                                                                           │  │
│  │ Silver layer: minute-granularity aggregates per (tenant, model, minute)│  │
│  │   Columns: tenant_id, model, minute_ts, req_count, error_count,        │  │
│  │            total_prompt_tokens, total_completion_tokens,               │  │
│  │            avg_latency_ms, p95_latency_ms                              │  │
│  │                                                                           │  │
│  │ Z-ORDER BY (tenant_id, model) — hot path for per-tenant filtering     │  │
│  │ OPTIMIZE runs every 15 min; compacts to ~128 MB data files             │  │
│  │                                                                           │  │
│  │ CDC semantics: MERGE WHEN MATCHED AND src.ts > tgt.ts                  │  │
│  │   (handles late-arriving events from network drops)                   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            GOLD QUERY LAYER                                   │
│                                                                              │
│  ┌────────────────────┐     ┌──────────────────────────────────────────┐   │
│  │ Dashboard Service   │     │ Databricks / Spark / Trino               │   │
│  │ (Grafana + API)    │────▶│                                           │   │
│  │                    │     │ Gold table: daily aggregates              │   │
│  │ SLA: p95 < 5 min   │     │   (tenant, date, model)                    │   │
│  │ refresh latency    │     │                                           │   │
│  └────────────────────┘     │ Z-ORDER BY tenant_id, model               │   │
│                              │ Partition BY date                         │   │
│                              │ Pre-computed cost_usd per tenant/day      │   │
│                              └──────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LIFECYCLE (FinOps)                           │
│                                                                              │
│  Bronze (raw, 7 days):     S3 Standard         ── $0.023/GB-mo               │
│  │                                                                      │   │
│  ├─▶ After 7 days:         S3 IA               ── $0.0125/GB-mo            │
│  │                        (no query needed)                              │   │
│  │                                                                      │   │
│  └─▶ After 30 days:        S3 Glacier Deep     ── $0.00099/GB-mo           │
│                            Archive                                        │   │
│                            (audit/compliance only)                          │   │
│                                                                              │
│  Silver (5-min aggregates, 90 days):  S3 Standard ── ~2 TB hot             │
│  Gold  (daily aggregates, 1 year):    S3 Standard ── ~3 GB hot            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Key Decisions, with Rejected Alternatives

### Decision 1: Delta Lake vs. Apache Iceberg as the table format

**Chosen: Delta Lake**

**Why:** The team already uses Delta Lake (proven in the Day 18 lab). Delta CDF (Change Data Feed) is the cleanest primitive for propagating micro-batch changes from Bronze to Silver without custom CDC logic. Deletion vectors let us handle failed/retried records atomically without rewriting full files. The Spark/Databricks ecosystem has mature Delta support for all three layers.

**Rejected — Apache Iceberg:** Iceberg has better open-vendor governance and superior metadata pruning for trillion-partition tables. However, Delta CDF has first-class support in our existing Databricks workspace, and the team has no Iceberg expertise. The governance advantage of Iceberg is irrelevant at this scale (500 tables, not 100,000). Iceberg's row-level DELETE performance (using remove files) is inferior to Delta's deletion vectors for the high-churn streaming workload at Bronze. Migrating to Iceberg would cost 2 weeks of engineering time for zero operational benefit at our table count.

**Rejected — Raw Parquet on S3:** No ACID semantics means concurrent stream writers will overwrite each other's data. No transaction log means no time travel, no CDF, no rollback capability. At 57,000 writes/sec, corrupted partitions are guaranteed within 24 hours.

---

### Decision 2: Tokenization strategy — where to redact PII

**Chosen: Tokenize at Bronze landing (SHA-256 HMAC), store both token and revocation mapping in a separate sealed vault table**

**Why:** PII must be redacted *before* any human — including on-call engineers during incident review — can read raw prompts. Doing tokenization at the stream consumer (Bronze write time) means PII never touches raw storage unredacted. HMAC-SHA256 with a per-tenant secret ensures that the same user always maps to the same token within a tenant, enabling joinability for cohort analysis without exposing the raw identifier. The revocation map lives in a sealed AWS KMS-backed table that only the compliance team can read.

**Rejected — Tokenize in Silver only:** This means raw Bronze stores plaintext PII for 7 days. If the Bronze bucket is misconfigured (even briefly), every raw prompt/response is exposed. Given Decree 13/2023/NĐ-CP equivalents and GDPR implications, this is an unacceptable risk window. Tokenizing later also means PII exists in the Kafka topic at rest, requiring Kafka ACLs and topic-level encryption — additional attack surface.

**Rejected — Format-Preserving Encryption (FPE):** FPE preserves data shape (e.g., a phone number still looks like a phone number), which is useful for downstream systems that need structural validation. However, FPE requires a symmetric key that must be accessible to every query engine that processes the data — making key management the weakest link. If the FPE key leaks, all PII is immediately reversible. HMAC-SHA256 is one-way: compromise of the key reveals *that* mapping but not others.

---

### Decision 3: Partitioning strategy — by date vs. by tenant

**Chosen: Partition by `(date, tenant_id)` using Hive-style partitioning**

**Why:** The 5-minute dashboard SLA requires filtering by tenant *and* time window simultaneously. A `(date, tenant_id)` compound partition allows the query planner to prune to exactly the tenant's partitions for the last N 5-minute windows — no full table scan. With 10,000 tenants, each partition has roughly 14,400 records/day (1B / 365 / 10,000), well within the recommended partition size range (1–10 GB).

**Rejected — Partition by tenant_id only:** A single tenant's daily data (~86M records, ~430 GB uncompressed) would be one monolithic partition. Querying the last hour of one tenant would read 430 GB and scan billions of rows. Compaction and Z-ordering would be ineffective.

**Rejected — No partitioning (hot path with Hudi or raw S3 listing):** S3 list operations cost $0.005 per 1,000 objects. With 5 TB/day split into ~4 MB Parquet files, that's 1.25 million files/day. One dashboard refresh (5 min) = ~4,340 S3 LIST operations. Across 1,000 concurrent dashboards: 4.3 million LIST calls/month = $21.50/month just for listing, before reading a single byte. Partitioning eliminates this by pointing the query engine directly at the relevant prefix.

---

### Decision 4: Streaming ingestion — Kafka + Flink vs. Kinesis + Lambda vs. Direct S3

**Chosen: Kafka (MSK) + Flink streaming job writing to Delta Bronze**

**Why:** Kafka provides durable, ordered, replayable ingestion with exactly-once semantics via transactional producers. Flink's checkpoint-based state management ensures no data loss if the writer crashes. At 57,870 req/sec with 5 KB messages, Kafka MSK `m5.2xlarge` × 3 brokers handles this comfortably (MSK handles ~1 MB/sec per partition; we need ~289 MB/sec; ~6 partitions is sufficient). The Kafka log retention of 7 days also serves as a secondary disaster-recovery buffer before the Bronze Delta tier.

**Rejected — Kinesis + Lambda:** Lambda cold starts add 100–500 ms latency per batch, causing backpressure at peak. Kinesis Enhanced Fan-out has a 2 MB/sec/consumer limit — at 289 MB/sec aggregate throughput, we'd need 145 shards at $0.015/hour = ~$104/month, plus Lambda invocation costs at this volume would exceed $5K/month alone. Kinesis also does not support transactional writes, so duplicate records in the Bronze layer would require expensive deduplication jobs.

**Rejected — Direct S3 writes from API services:** This bypasses Kafka entirely. Each API pod would write directly to S3, partitioning and writing Parquet. Problem: API pods are stateful but not coordinator-aware — they'd need distributed locking or a write-ahead log to prevent concurrent writes to the same partition from overwriting each other. At 57,000 req/sec, race conditions in direct S3 writes cause data loss. Kafka abstracts this: producers are fire-and-forget, Kafka handles ordering and durability, and the Flink consumer is the sole writer to Delta.

---

### Decision 5: Lifecycle tiering — how to stay under $5K/month

**Chosen: S3 Standard → S3 IA (7 days) → S3 Glacier Deep Archive (30 days), with Delta Lake native tiering via `ALTER TABLE SET TBLPROPERTIES`**

**Why:** Delta Lake 3.0+ supports `ALTER TABLE [name] SET TBLPROPERTIES ('delta.targetFileSize' = '128mb', 'delta.dataSkippingNumIndexedCols' = '20')` and the storage tier is managed via S3 Object Lifecycle Rules on the bucket, not at the Delta level. This is zero-engineering: configure lifecycle rules on the S3 bucket prefixes. The Delta transaction log (Parquet + JSON) in `_delta_log/` is also covered by the same lifecycle rules.

| Tier | Duration | Raw Bytes/day | Compressed Bytes/day | Monthly Storage | Cost |
|---|---|---|---|---|---|
| S3 Standard (Bronze) | 0–7 days | 5 TB | 1.25 TB | 8.75 TB avg | ~$200/mo |
| S3 IA (Bronze) | 7–30 days | — | 1.25 TB | 28.75 TB avg | ~$360/mo |
| S3 Glacier DA | 30+ days | — | 1.25 TB | 365 TB (all time) | ~$361/mo |
| Silver (aggregates) | 90 days | 50 GB | 12.5 GB | 1.1 TB avg | ~$25/mo |
| Gold (daily agg) | 365 days | 0.5 GB | 0.125 GB | 45 GB avg | ~$1/mo |
| **Total** | | | | | **~$947/mo** |

The $947/month estimate leaves **$4,053 headroom** for compute (Flink, Databricks serverless, S3 LIST costs, Cross-Region Replication). Under the $5K ceiling with comfortable margin.

**Rejected — Store everything in S3 Standard indefinitely:** 365 days × 1.25 TB = 456 TB × $0.023 = **$10,488/month** — more than 2× the budget. Impossible.

**Rejected — Tier to Glacier immediately:** S3 Glacier has a 128 KB minimum billable object size and a 90-day minimum storage duration charge. Bronze files at ~4 MB average are above the minimum, but the 90-day minimum means data stored for 7 days still bills for 90 days. S3 Glacier Deep Archive's 180-day minimum is even worse for our 7-day hot retention requirement. S3 IA's 30-day minimum is the best fit for the first transition.

---

### Decision 6: Catalog choice — Unity Catalog vs. Hive Metastore vs. AWS Glue

**Chosen: Unity Catalog (Databricks)**

**Why:** Unity Catalog provides column-level security — critical for a system where tenant A's data must never be readable by tenant B, and where the compliance team needs to audit who read PII columns. UC's lineage graph automatically tracks which Gold table was derived from which Bronze table, enabling the "which models break if I drop this column?" query required by feature store teams. Time travel queries (`SELECT * FROM table VERSION AS OF 3`) work out of the box with UC-managed tables.

**Rejected — Hive Metastore:** HMS has no column-level security (only table-level). With 500+ tables and 10,000 tenants requiring row/column-level isolation, HMS would require workarounds (views, Spark UDFs) that don't compose cleanly and add query latency.

**Rejected — AWS Glue Data Catalog:** Glue has no built-in lineage. Cross-account access for teams in different AWS accounts requires complex IAM policies. Glue's crawler-based schema inference is too slow for our 1B/day ingestion rate — it would constantly lag behind the data.

---

## 4. Failure Modes

### FM-1: Bronze write pipeline backs up during a Kafka broker outage

**Scenario:** A Kafka broker fails and MSK takes 10 minutes to elect a new leader. During this window, the Flink checkpoint is frozen. The Flink job continues from the last successful checkpoint, but 2 minutes of records (at 57,870 req/sec × 120 sec = 6.9M records, ~35 GB) are replayed from Kafka.

**Detection:**  
- Flink metrics: `consumer.lag` > 1M records triggers PagerDuty alert.  
- S3 Bronze write rate drops below 1 TB/hour threshold (CloudWatch alarm).  
- Dashboard refresh latency exceeds 10 minutes (Grafana alert).

**Rollback:**  
Because Kafka is the source of truth with 7-day retention, the Flink job replays from the last checkpoint offset. The Delta transaction log records each file as a separate commit. No data loss occurs — just a processing delay. After Kafka recovers, the Flink job catches up within 20 minutes (3× normal throughput using backpressure relief). Bronze Delta Lake's ACID semantics ensure that replayed records don't corrupt existing files — they create new Parquet files or merge into existing ones via `MERGE WHEN NOT MATCHED`.

**Day 18 tie-in:** This relies on Delta's ACID commit semantics. Each Flink micro-batch commits as a single Delta transaction. On replay, the same records either create new files (idempotent) or are deduplicated in Silver via the `request_id` deduplication step.

---

### FM-2: Late-arriving events corrupt Silver aggregates after OPTIMIZE

**Scenario:** A network partition in a remote province causes GPS/telemetry events to arrive 3 hours late. The Silver micro-batch at T+5 min computed `total_tokens = 1,000,000` for a tenant's 5-minute window. The late events arrive at T+3h and join the next micro-batch, but this *adds* to the running total rather than correcting the prior 5-minute aggregate. After `OPTIMIZE` compacts the Silver table, the incorrect value is baked into compacted files.

**Detection:**  
- Per-tenant anomaly detection: if `total_tokens` for a 5-minute window changes by >20% between consecutive reads, flag as suspicious (Apache Atlas lineage alert).  
- Cross-layer validation: Bronze raw record count vs. Silver aggregate sum should match within 0.1%. A nightly reconciliation job catches drift.

**Rollback:**  
Delta time travel is the recovery mechanism. The last known-good Silver table version is restored:

```python
# Restore Silver to version 452 (5 minutes before anomaly detected)
dt = DeltaTable("s3://lakehouse/silver/llm_calls")
dt.restore_to_version(452)  # Delta time travel restore
```

After restore, the late events are reprocessed from Bronze using the `src.ts > tgt.ts` merge condition in the Silver job, which correctly updates the affected 5-minute windows. `OPTIMIZE` is re-run after the correction batch completes.

**Day 18 tie-in:** This failure mode directly uses Delta time travel (`restore_to_version`). Without time travel, this corruption would require a full re-ingest from Bronze — 5 TB × 7 days = 35 TB re-process, 6+ hours of downtime.

---

### FM-3: PII tokenization key rotation breaks tenant cohort analysis

**Scenario:** The compliance team rotates the HMAC-SHA256 key for tenant `t_12345`. After rotation, new requests from that tenant get new tokens. The cohort analysis query (which joins all records by tenant_token) now sees a gap: old tokens and new tokens don't join, making it appear that `t_12345` had zero activity for the rotation window.

**Detection:**  
- Token entropy check: if a tenant's distinct token count spikes by >50× during a key rotation window, the lineage join will silently return fewer rows than expected.  
- Weekly reconciliation: compare Bronze record count (by `request_id`) vs. Gold aggregate count (by `tenant_token`). A >1% discrepancy triggers an alert.

**Rollback:**  
Key rotation is a planned operation, not an emergency. Before rotation, the old key's token mapping is exported to a frozen `tenant_token_history` table. The rotation plan:

1. Snapshot old key's token → PII mapping to `token_vault_v1` (sealed, immutable).
2. Rotate to new key, begin issuing `v2` tokens.
3. Re-derive `v1` cohort analysis using the frozen snapshot.
4. Backfill: re-run Bronze tokenization for the last 7 days using the new key, producing `v2` tokens for historical records.
5. Delete `v1` tokens from the vault (compliance requirement — old tokens must not be reversible after 90 days).

If step 4 fails mid-way, Delta schema evolution handles the `token_v2` column addition without dropping `token_v1`, preserving both mappings.

**Day 18 tie-in:** Delta schema evolution (`schema_mode="merge"`) handles the addition of `token_v2` to the Bronze schema without rewriting existing files. The existing `token_v1` column is preserved and immutable.

---

## 5. Cost Back-of-Envelope

### Storage (monthly)

Raw data: 1B req/day × 5 KB/req = **5 TB/day logical**

After Delta compression (Parquet, Snappy, ~4× for JSON-structured data): **1.25 TB/day physical**

| Component | Storage | Duration | Tier | $/GB-mo | $/month |
|---|---|---|---|---|---|
| Bronze raw | 1.25 TB/day | 0–7d avg (8.75 TB) | S3 Standard | $0.023 | $201 |
| Bronze warm | 1.25 TB/day | 7–30d avg (28.75 TB) | S3 IA | $0.0125 | $359 |
| Bronze cold | 1.25 TB/day | 30–365d (456 TB-mo) | Glacier DA | $0.00099 | $451 |
| Silver agg | 12.5 GB/day | 90d avg (1.125 TB) | S3 Standard | $0.023 | $26 |
| Gold daily | 0.125 GB/day | 365d avg (45.6 GB) | S3 Standard | $0.023 | $1 |
| Delta logs | 1% overhead | — | same as parent | — | ~$10 |
| **Storage subtotal** | | | | | **~$1,048/mo** |

### Compute (monthly)

| Component | Spec | Monthly cost |
|---|---|---|
| MSK Kafka (m5.2xlarge × 3, 6 partitions) | $0.46/hr × 3 × 730 | $1,007/mo |
| Flink on EMR (r6i.2xlarge × 4, spot) | $0.42/hr × 4 × 730 × 0.7 | $865/mo |
| Databricks (DBU for Gold batch, 50K rows/day) | ~2K DBU × $0.15 | $300/mo |
| S3 LIST requests (4.3M/month at $0.005/1K) | — | $22/mo |
| S3 GET requests (dashboard reads) | ~50M GET/mo × $0.0004/1K | $20/mo |
| Cross-region replication (Bronze → DR region) | 5 TB/day × $0.002/GB | $300/mo |
| **Compute subtotal** | | **~$2,514/mo** |

### Total

| Category | $/month |
|---|---|
| Storage | $1,048 |
| Compute | $2,514 |
| **Total** | **$3,562/mo** |

**Margin to budget: $1,438/month (28.8% headroom).** The headroom covers Kafka burst scaling events, Databricks interactive query compute for on-call engineers, and S3 ANALYTICS requests for cost optimization reports.

---

## 6. One-Week MVP Slice

The MVP proves the hardest parts first — the parts that could invalidate the architecture.

### MVP scope: Bronze ingestion + tokenization + 5-min dashboard query

**What to build:**

1. **Kafka producer stub** — a Python script that generates synthetic LLM calls (1K req/sec is sufficient for MVP) with realistic schema: `{request_id, ts, tenant_id, user_id, model, prompt_tokens, completion_tokens, latency_ms, status, raw_prompt, raw_response}`. No real LLM needed.

2. **Flink Bronze writer** — a minimal Flink job that:
   - Consumes from Kafka topic
   - Runs HMAC-SHA256 tokenization on `user_id` and `tenant_id`
   - Writes to Delta Bronze partitioned by `(date, tenant_id)`
   - Commits every 60 seconds (not 5 min — faster iteration)

3. **Silver micro-batch** — a scheduled Spark job (cron every 5 min) that:
   - Reads from Bronze using `delta_scan()`
   - Deduplicates by `request_id`
   - Aggregates to minute granularity
   - Writes to Silver with `MERGE WHEN MATCHED AND src.ts > tgt.ts` for late data
   - Runs `OPTIMIZE ZORDER(tenant_id, model)` every 30 min

4. **Gold dashboard query** — a single Databricks SQL cell that:
   - Queries Silver for the last hour of one tenant
   - Computes cost_usd using the model price table
   - Returns p50/p95 latency and total spend
   - Executes in < 5 seconds for the MVP scale

**What NOT to build in week 1:**
- Multi-region replication
- Key rotation infrastructure
- Column-level security (UC setup)
- Anomaly detection pipeline
- The full Grafana dashboard

**MVP success criteria:**
- Bronze table has 1M+ records, tokenized correctly (verify: `SELECT COUNT(DISTINCT user_token) FROM bronze` returns expected cardinality).
- Silver 5-min aggregates match Bronze raw counts within 0.1%.
- Dashboard query returns in < 10 seconds for 1 week of data.
- End-to-end data freshness: record written to Kafka → appears in Gold dashboard in < 10 minutes.

The MVP validates three critical assumptions: (1) Kafka → Delta Bronze write throughput is sufficient, (2) tokenization overhead is < 5% of Flink CPU, and (3) Silver aggregates are accurate. If any of these fail, the architecture changes — before any other work is invested.
