# 🔬 XTD RESEARCH LABS CASE STUDY: Asynchronous Ingestion & PySpark Medallion Pipeline  
*A production-grade data engineering pipeline I designed and built for a UK grid decarbonization research scenario, processing three years of regional carbon intensity data through async API ingestion, distributed transformation, and a Bronze/Silver/Gold lakehouse architecture.*

---

## 🏷️ Badges  
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![PySpark](https://img.shields.io/badge/PySpark-4.1.1-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-blue)
![Async](https://img.shields.io/badge/Ingestion-aiohttp%20async-yellow)
![Medallion](https://img.shields.io/badge/Architecture-Medallion-success)
![Days Ingested](https://img.shields.io/badge/Days-1%2C095-red)
![Silver Rows](https://img.shields.io/badge/Silver%20Rows-945K-purple)

---

# 🏢 The Client Scenario

**XTD Research Labs** is a fictional scientific institution dedicated to understanding the environmental footprint of electricity generation in the United Kingdom. Their analysts and policy researchers depend on regional carbon intensity data to build longitudinal climate models, with their current focus covering the years 2022 to 2024.

The lab's existing pipeline was an in-memory Python script designed for 30-minute interval pulls. It worked for single-day ingestion but became the binding constraint on every research initiative requiring more than a few days of history. Multi-year backfills timed out. Concurrent API requests triggered rate-limiting. Raw responses were transformed in flight and discarded, so re-running historical analysis with new research parameters meant re-pulling from the source API every time.

I was engaged as the Data Engineer responsible for transitioning XTD from this single-day ingestion model to a distributed big data architecture capable of handling thousands of days of regional data across the full 2022 to 2024 research window.

> *This project was built against a structured case-study brief from an ACTD-accredited (American Council of Training and Development) data engineering scenario.*

---

# 🎯 The Business Problem

The brief identified three specific engineering constraints blocking XTD's research:

1. **Scale Incompatibility.** The in-memory pipeline could not process 1,000+ days of regional data in a single run. It was architected for daily incremental pulls, not historical backfills.
2. **Network Latency.** Synchronous API requests against the Carbon Intensity API led to connection timeouts and rate-limiting when fetching multi-year datasets.
3. **No Schema Preservation.** Without a Bronze/Silver/Gold layered architecture, raw responses were discarded after transformation. Re-running analysis with new research parameters required re-fetching from the source API, which was slow and put the lab at risk of rate-limiting from the upstream provider.

Left unaddressed, these problems would lead to incomplete climate models and weakened policy recommendations. The brief required a pipeline that could handle the full three-year backfill reliably, archive raw historical data immutably, and process nested JSON at scale.

---

# 🌟 What I Built

A three-stage data engineering pipeline that delivers on the three pillars XTD's brief specified:

1. **High-Concurrency Async Ingestion.** An `aiohttp`-based async extractor with semaphore-bounded concurrency, exponential backoff retry, and file-existence idempotency. Pulls 1,095 days of regional intensity data from the UK Carbon Intensity API into a Bronze data lake.
2. **Distributed PySpark Transformation.** A medallion-architecture transform layer that reads raw JSON from Bronze, explodes deeply nested arrays (regions, generation mix), pivots fuel types into typed columns, and aggregates to daily research metrics. Incremental Silver writes, full-rebuild Gold semantics.
3. **Schema-Owned-by-SQL Load.** A two-stage dedup-merge loader that pulls Gold CSVs and lands them into a PostgreSQL `carbon_data` schema declared by hand-authored DDL with composite PRIMARY KEY discipline.

End-to-end, the pipeline processes **8.7 million intermediate rows down to 19,728 daily aggregated research metrics** across 14 UK regions and 3 years, in roughly 19 minutes on a single laptop. Re-runs on populated data complete in seconds thanks to layered idempotency.

---

# 🧠 What I Demonstrate in This Project

### 🔹 Asynchronous I/O for High-Concurrency Ingestion  
I designed an `aiohttp`-based extractor using `asyncio.Semaphore(5)` and `aiohttp.TCPConnector(limit=5)` for two layers of rate-limit defense. The semaphore bounds in-flight task count; the TCP connector bounds open socket count. Together they let me hit 1,095 days of API calls in 11m 43s without ever triggering a 429 from the upstream provider.

### 🔹 Production-Grade Retry with Exponential Backoff  
Every API request gets three attempts. On a 429, the backoff is 5s, 10s, 15s. On other HTTP errors, 2s. On network errors (timeouts, connection drops), 2s scaled by attempt. **In the actual ingestion run, 32 transient HTTP 500s came back from the API. All 32 recovered. Zero permanent failures.**

### 🔹 File-Existence Idempotency  
Each successful API response is written to `data/bronze/{date}.json`. Before any new request, the extractor checks if the file already exists and skips if so. Result: the first run takes 11m 43s. Re-running the extractor on a populated Bronze takes 4 seconds, hitting the API zero times. Safe to interrupt and resume.

### 🔹 Defensive Handling of API Edge Cases  
The Carbon Intensity API occasionally returns `200 OK` with an empty payload (`data: []`). Naive code would save the empty response and call it success. My check (`if 'data' in data and data['data']`) catches this, logs a warning, and refuses to write a corrupted bronze file. In the actual run, this caught date `2023-10-22`, which is why Bronze contains 1,095 days, not 1,096. Honest count, not a fake one.

### 🔹 Medallion Architecture with Three Idempotency Models  
- **Bronze** (raw JSON): idempotent via file-existence check
- **Silver** (Parquet): idempotent via `_processed_files.txt` tracker — only new JSON files get folded in
- **Gold** (CSV): idempotent via wipe-and-rebuild — always reflects the current full Silver state

Three layers, three different idempotency strategies, each appropriate for that layer's cost profile and update pattern. Bronze is expensive to rebuild (API rate-limits), so it's incremental. Gold is cheap to rebuild (a `groupBy` over 945K Silver rows), so it's always fresh.

### 🔹 PySpark Schema Flattening at Scale  
The Carbon Intensity API returns deeply nested JSON: `data[].regions[].generationmix[]`. I unfold two levels with `F.explode` (regions, then generation mix), pivot fuel types from rows into 9 columns (biomass, coal, gas, hydro, imports, nuclear, other, solar, wind), and group back to one row per region per timestamp. **53,594 raw JSON records expand to 8,682,228 intermediate rows after the double explode, then collapse to 945,092 silver records after the pivot.** This is exactly the operation pandas would handle poorly at this scale; PySpark is the right tool.

### 🔹 Two-Stage Dedup-Merge for Idempotent Loads  
The loader does deduplication twice. First, it removes duplicates within the input files using `pandas.drop_duplicates(subset=["regionid", "date_recorded"])`. Then it pulls existing keys from Postgres and does a `merge(how="left", indicator=True)` to drop rows already in the database. Only `left_only` rows get appended. Result: first load inserts 19,728 rows; second load on the same data inserts zero. Layered defense, same composite key as the Postgres PRIMARY KEY.

### 🔹 Schema Owned by SQL, Not Inferred by Pandas  
DDL lives in `sql/create_table.sql` with a composite `PRIMARY KEY (regionid, date_recorded)`, four `NOT NULL` columns, and `DECIMAL(10,2)` precision on all 9 fuel columns plus the intensity average. Pandas isn't deciding the schema; the schema is declarative, versioned in git, and the load layer's dedup logic mirrors the PK exactly. Defense in depth.

### 🔹 Fail-Fast Environment Validation  
`etl/db_config.py` checks all required env vars at import time. If `DB_USER`, `DB_PASSWORD`, `DB_HOST`, or `DB_NAME` is missing, the pipeline refuses to start. No silent failures 10 minutes into a Spark transform because the password wasn't set.

### 🔹 Developer-Workflow Wipe Modes  
`etl/wipe_all.py` has five modes: `bronze`, `silver`, `gold`, `all`, and `full`. The interesting one is **`all`**: it's the "safe reset" that wipes Silver, Gold, and the Spark temp directory but **deliberately preserves Bronze**. Bronze is expensive to rebuild (1,095 API calls). `full` is the nuclear option, gated behind a `WARNING`-level "💀 DANGER" log. Different defaults for different rebuild costs.

---

# 🌐 High-Level Architecture Diagram

```
                ┌─────────────────────────────────────┐
                │      UK Carbon Intensity API        │
                │  api.carbonintensity.org.uk         │
                │  /regional/intensity/{date}/pt24h   │
                └─────────────────┬───────────────────┘
                                  ▼
                ┌─────────────────────────────────────┐
                │            🥉  BRONZE               │
                │       (async aiohttp · 5 conc.)     │
                │  • 1,095 daily JSON files           │
                │  • file-existence idempotency       │
                │  • exponential backoff retry        │
                │  • empty-payload defense            │
                └─────────────────┬───────────────────┘
                                  ▼
                ┌─────────────────────────────────────┐
                │            🥈  SILVER               │
                │            (PySpark 4.1.1)          │
                │  • read JSON from Bronze            │
                │  • explode regions array            │
                │  • explode generationmix array      │
                │  • pivot 9 fuel types to columns    │
                │  • incremental via tracker file     │
                │  • Parquet, append mode             │
                │  • 945,092 records                  │
                └─────────────────┬───────────────────┘
                                  ▼
                ┌─────────────────────────────────────┐
                │             🥇  GOLD                │
                │       (PySpark daily aggregate)     │
                │  • groupBy (regionid, date)         │
                │  • mean intensity, mode index       │
                │  • mean per-fuel mix percentage     │
                │  • full-rebuild on every run        │
                │  • CSV output                       │
                │  • 19,728 daily research records    │
                └─────────────────┬───────────────────┘
                                  ▼
                ┌─────────────────────────────────────┐
                │       PostgreSQL Warehouse          │
                │  carbon_data.carbon_intensity_daily │
                │  • Composite PK (regionid, date)    │
                │  • DECIMAL(10,2) precision          │
                │  • 4 NOT NULL identifier columns    │
                │  • Two-stage dedup-merge on load    │
                └─────────────────┬───────────────────┘
                                  ▼
                ┌─────────────────────────────────────┐
                │     BI / Research Consumers         │
                │   (Tableau, Power BI, Jupyter)      │
                └─────────────────────────────────────┘
```

---

# 🗂 Project Structure

```
xtd_research_labs_case_study/
├── data/
│   ├── bronze/                      # 1,095 raw API JSONs (gitignored)
│   ├── silver/                      # Parquet intermediate (gitignored)
│   └── gold/                        # Daily aggregate CSV (gitignored)
│
├── etl/
│   ├── db_config.py                 # Env-var loader with fail-fast validation
│   ├── extract.py                   # Async aiohttp extractor
│   ├── transform.py                 # PySpark medallion: Bronze → Silver → Gold
│   ├── load.py                      # Postgres loader with two-stage dedup-merge
│   ├── logger.py                    # Cross-platform logging framework
│   └── wipe_all.py                  # 5-mode developer wipe utility
│
├── logs/
│   └── pipeline.log                 # Rotating run logs (UTF-8)
│
├── sql/
│   └── create_table.sql             # DDL: composite PK, DECIMAL precision
│
├── .env                             # Credentials (gitignored)
├── .gitignore
├── README.md
└── requirements.txt
```

---

# 🔄 Pipeline Flow

This pipeline runs as **three explicit stages**. There is no `run_all.py` orchestrator. Each layer is invoked manually, in order. This is deliberate: each layer has different runtime characteristics (Bronze is API-bound, Silver and Gold are CPU-bound, Load is DB-bound), and decoupling them makes debugging, partial reruns, and external orchestration (cron, Airflow) cleaner.

### 1️⃣ Extract — Async Bronze Ingestion  
`python -m etl.extract`  
The async extractor iterates from 2022-01-01 through 2024-12-31 (1,096 calendar days). For each date, it checks whether `data/bronze/{date}.json` already exists. If yes, the request is skipped. If no, an `aiohttp` request is fired against the API, bounded by a semaphore of 5 concurrent in-flight requests and a TCP connector limit of 5 sockets. On a 200 with non-empty payload, the response is written to disk. On a 429, exponential backoff (5s, 10s, 15s) over three attempts. On other failures, retried twice. On empty payload, logged as a warning and skipped without writing a corrupted file.

### 2️⃣ Transform — PySpark Bronze → Silver → Gold  
`python -m etl.transform`  
Spark session boots with 14g driver memory, 14g executor memory, KryoSerializer, 64 shuffle partitions, and the Postgres JDBC driver registered. Cold-start cost is about 1m 50s.  

**Silver layer (incremental):** reads `_processed_files.txt` to find new Bronze files, loads only those into Spark, explodes the regions array and the generationmix array, pivots fuel types into 9 typed columns, appends to the Silver Parquet directory. Already-processed files are never re-read.  

**Gold layer (full rebuild):** reads the entire Silver dataset (945K records on full run), aggregates with `groupBy(regionid, date_recorded)`, computes mean intensity (`F.round(F.mean, 2)`), mode index, and rounded mean per-fuel mix percentages. Result: 19,728 daily research records, written as CSV in overwrite mode.

### 3️⃣ Load — Postgres with Dedup-Merge  
`python -m etl.load`  
The loader globs `data/gold/gold_carbon_historical.csv/part-*.csv` (Spark writes CSV as a directory of part-files), concatenates with pandas, and runs two-stage deduplication: first removes duplicates within the input via `drop_duplicates`, then pulls existing `(regionid, date_recorded)` keys from Postgres and removes any rows already in the database via a left-join indicator merge. Only `left_only` rows are appended. The first load inserts 19,728 rows in 8 seconds; subsequent loads on the same data insert zero.

---

# 📊 Real Performance Numbers (Actual Run, May 2026)

These come from the actual `pipeline.log` file, not estimates.

| Stage | First Run | Idempotent Re-run |
|---|---|---|
| Extract (1,095 days) | **11m 43s** | **4 seconds** |
| Transform — Spark startup | ~1m 50s | ~1m 50s |
| Transform — Silver + Gold | **5m 32s** | seconds (no new files) |
| Load — Gold to Postgres | **8 seconds** | **2 seconds** (zero inserts) |
| **End-to-end (cold)** | **~19 minutes** | **~2 minutes** (mostly Spark startup) |

### Volume at Each Layer

| Layer | Records | Notes |
|---|---|---|
| Bronze | 1,095 JSON files | 1 day skipped (empty API payload, logged as warning) |
| Silver — raw read | 53,594 records | Pre-explode, one row per region per timestamp |
| Silver — after explode | 8,682,228 rows | After exploding regions × generation mix |
| Silver — after pivot | 945,092 rows | After pivoting 9 fuel types back to columns |
| Gold | 19,728 daily records | 14 regions + 4 zones × 3 years × ~daily aggregation |
| Postgres warehouse | 19,728 rows | Same as Gold; composite PK enforced |

### API Reliability (Actual Numbers from the Run)

| Metric | Value |
|---|---|
| Total API calls attempted | ~1,096 |
| Successfully saved | 1,095 |
| Empty-payload responses (caught and skipped) | 1 (`2023-10-22`) |
| Transient HTTP 500 errors during extraction | **32** |
| 500 errors that retried + recovered | **32 (all of them)** |
| HTTP 429 rate-limit hits | **0** |
| Permanent failures after 3 retry attempts | **0** |

The retry-with-backoff logic isn't just a claim. It actually saved 32 days of data that would have been lost in a naive implementation.

---

# 🔑 Key Engineering Decisions

### 1️⃣ Why async instead of synchronous requests?  
Synchronous requests with the `requests` library would have taken 4 to 6x longer than the actual 11m 43s. Each daily request takes 0.2 to 2 seconds depending on the server's load. With 1,096 calls done one at a time, that's 30 to 50 minutes minimum. Async with a semaphore of 5 turns this into 11m 43s without ever tripping rate limits.

### 2️⃣ Why two layers of rate-limit defense (semaphore + TCP connector)?  
The semaphore (`asyncio.Semaphore(5)`) bounds **logical task concurrency**. The TCP connector (`aiohttp.TCPConnector(limit=5)`) bounds **physical socket count**. Either one alone would mostly work, but layered they make a 429 essentially impossible for this API at this scale. The actual run produced zero 429s across 1,000+ calls.

### 3️⃣ Why three idempotency models, one per layer?  
The cost of rebuilding each layer is different. Bronze costs 11 minutes and 1,000+ API calls. Silver costs 5 minutes of Spark. Gold costs 3 seconds. So:
- Bronze: skip files that exist (cheap to check, expensive to rebuild)
- Silver: tracker file of processed Bronze files (allows incremental Silver growth)
- Gold: wipe and rebuild every run (cheap, guarantees freshness)

A unified "wipe everything and rebuild" model would force every run to re-hit the API. A unified "incremental everything" model would make Gold queries return stale data. Different mechanisms, each appropriate for its layer.

### 4️⃣ Why pandas dedup-merge instead of `ON CONFLICT DO NOTHING`?  
PostgreSQL's `ON CONFLICT (regionid, date_recorded) DO NOTHING` would be the conventional choice. I used a two-stage pandas dedup-merge instead because it gives explicit logging on both axes: "removed X rows from input files" and "skipped Y rows already in database." When something goes wrong, the log tells the operator exactly where the duplicate came from. Loss of one Postgres optimization in exchange for observability that helps you debug an idempotent load.

### 5️⃣ Why composite PK on `(regionid, date_recorded)`?  
Natural keys for the data. There is exactly one row per (region, day) in the analytics warehouse. The PK enforces this at the database level. It's also the same key the load layer dedups against, so PostgreSQL's constraint matches the application logic exactly. Defense in depth.

### 6️⃣ Why DECIMAL(10,2) on fuel mix percentages instead of FLOAT?  
Same reasoning as money in fintech: floats lose precision (`0.1 + 0.2 != 0.3`), and fuel mix data is consumed by analysts who will aggregate it. `DECIMAL(10,2)` preserves exact two-decimal precision through `SUM`, `AVG`, and `MEAN` operations. Carbon intensity is reported in gCO2/kWh and fuel mix in percentages — both demand exactness for research-grade work.

### 7️⃣ Why no `run_all.py` orchestrator?  
Each layer has a meaningfully different runtime profile (Bronze is API-bound, Transform is CPU-bound and JVM-heavy, Load is DB-bound). Running them as separate commands means:
- A failed extract doesn't roll back a working transform
- The transform layer can be re-run against existing Bronze for debugging without re-hitting the API  
- External orchestrators (cron, Airflow, Prefect) can wire stages independently with their own retry policies
- Cold Spark startup happens once per transform invocation, not bundled with the much faster extract

For production deployment, an external scheduler is the right place for orchestration. For development, three explicit commands is honest about what's happening.

---

# 🧩 The Carbon Intensity API

This project pulls from the [UK National Grid ESO Carbon Intensity API](https://carbon-intensity.github.io/api-definitions/), a free, public, government-maintained service that provides 30-minute interval carbon intensity data for 14 UK regions plus 4 national zones.

The endpoint used: `https://api.carbonintensity.org.uk/regional/intensity/{date}/pt24h`

Each daily response is a deeply nested JSON containing 48 half-hour intervals × 18 regional readings × up to 9 fuel-type breakdowns. That's the structure the Silver layer flattens.

This is a **real public API**, not a Kaggle dataset. The pipeline is hitting a live production service every time Bronze is regenerated, which is part of why retry and rate-limit discipline matter.

---

# 🧠 Custom Logging & Observability

The `etl/logger.py` module is a portable observability layer I built across multiple data engineering projects (you'll see the same file in [FibbieBanks](https://github.com/yoismail/fibbie_banks) and the ChocoDelight data platform). It includes:

- **Cross-platform UTF-8 detection** that checks for VS Code Terminal, Windows Terminal, PowerShell 7+, and Windows code page 65001 before deciding whether emojis are safe to print
- **Dual-handler setup**: colorized console output plus a rotating UTF-8 file handler with no ANSI escape codes
- **Message-only colorization** so timestamps stay clean even in colored console output
- **`section(title)`** for visual banners between pipeline stages
- **`@timed` decorator** that wraps any function and logs its execution duration in human-readable form ("Step completed in 5m 32s")
- **SQLAlchemy noise suppression** to keep engine logs out of pipeline output

Result: one `pipeline.log` file tells the whole story of any run, with timestamps, row counts, retry behavior, and per-stage durations. The retry recoveries described above came directly from this log.

---

# ▶️ Running the Pipeline

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
Create a `.env` file in the project root:
```
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=xtd_research
```
The pipeline will refuse to start if any of `DB_USER`, `DB_PASSWORD`, `DB_HOST`, or `DB_NAME` is missing.

### 3. Set up Spark dependencies (Windows-specific)
This project assumes:
- Hadoop binaries at `C:/hadoop`
- Spark Postgres JDBC driver at `C:/spark/jars/postgresql-42.7.4.jar`
- Spark temp directory at `C:/spark_temp`

These paths are hardcoded in `etl/transform.py` for transparency. For cross-platform use, they should be env-driven (see "Future Iterations" below).

### 4. Create the Postgres warehouse table
```bash
psql -U postgres -d xtd_research -f sql/create_table.sql
```
This creates the `carbon_data` schema, drops `carbon_intensity_daily` if it exists, and recreates it with the composite PK and DECIMAL precision discipline.

### 5. Run the three stages in order
```bash
python -m etl.extract       # Async Bronze ingestion (~12 min cold, 4 sec warm)
python -m etl.transform     # PySpark Silver + Gold (~7 min cold incl. Spark startup)
python -m etl.load          # Postgres dedup-merge load (~8 sec cold, 2 sec idempotent)
```

### 6. Reset for re-runs
```bash
python -m etl.wipe_all bronze    # Just the Bronze layer
python -m etl.wipe_all silver    # Just the Silver layer  
python -m etl.wipe_all gold      # Just the Gold layer
python -m etl.wipe_all all       # Safe reset: Silver + Gold + Spark temp, KEEPS Bronze
python -m etl.wipe_all full      # Nuclear: deletes everything including Bronze
```

The `all` mode deliberately preserves Bronze because rebuilding it costs 11 minutes and 1,000+ API calls. `full` is gated behind a `WARNING`-level danger log.

---

# 🔮 Future Iterations

Honest gaps and what I'd ship next if I were extending the project:

- **Cross-platform Spark paths.** The current `transform.py` hardcodes `C:/hadoop` and `C:/spark/jars/...`. Should be env-driven so the same code runs on macOS, Linux, and Windows.
- **Tracker-file resilience.** The `_processed_files.txt` tracker is updated *before* the Spark append completes. If Spark crashes mid-write, the file gets marked as processed but its data isn't in Silver. A transactional version (write Silver successfully → then mark file processed) would close this gap.
- **`run_all.py` orchestrator.** For non-interactive runs (cron, CI), a thin orchestrator that runs the three stages sequentially with halt-on-failure semantics. Each stage already does the right thing in isolation; orchestration is just composing them.
- **A `dim_region` lookup table.** Currently `shortname` and `dno` are denormalized into every fact row. A small `carbon_data.dim_region` table with FK from the fact would normalize the schema and reduce row width. Cost is tiny (~15K rows × small saving), but it's the right Kimball shape.
- **Secondary indexes on the fact table.** The composite PK gives fast lookups on `(regionid, date_recorded)`. A standalone index on `date_recorded` would speed up time-range queries that span all regions.
- **API token / authentication layer.** The Carbon Intensity API is currently unauthenticated, but if the API ever requires keys, the rate-limit math changes meaningfully and the retry logic needs a per-token backoff.
- **Spark cluster deployment.** Current `transform.py` uses `spark.driver.host=localhost` for single-node execution. The same code would run on a managed cluster (Databricks, EMR, GCP Dataproc) with config changes only.
- **dbt models on top of Gold.** Analyst-authored transformations layered on top of the warehouse, with tested SQL models for common research queries.

---

# 🧪 Honest Limitations

A few things this project does NOT do, listed honestly:

- **No automated tests.** All testing was manual via real pipeline runs and log inspection.
- **No Spark unit tests.** The transform logic is verified by the log's row-count assertions (53K → 8.7M → 945K → 19.7K), not by `pytest` fixtures.
- **Single-node Spark.** The Spark config is tuned for a laptop with 16GB+ RAM. Running on smaller machines would require reducing the driver/executor memory settings.
- **No retry on the load layer.** If the Postgres connection drops mid-insert, the load fails. The dedup-merge will catch what got partially inserted on the next run, but transactional rollback is not currently implemented.
- **`requirements.txt` is not fully pinned.** Only PySpark is pinned to `4.1.1`. For full reproducibility, `pip-tools` or `poetry` would lock the dependency tree.

These are real engineering trade-offs for a portfolio project at this scope, not bugs. The "Future Iterations" section above is where they'd be addressed.

---

# 🤝 Contributing

If you'd like to contribute, feel free to:

1. Fork the repo
2. Create a feature branch
3. Commit changes
4. Open a pull request

---

# 📄 License

This project is released under the **MIT License**.

---

# 👤 Author

**Yomi Ismail**  
Data Engineer · Suffolk, UK  
[LinkedIn](https://www.linkedin.com/in/yomi-ismail) · [GitHub](https://github.com/yoismail) · [Portfolio](https://yoismail.github.io/portfolio/)

[![GitHub](https://img.shields.io/badge/GitHub-yoismail-black?logo=github)](https://github.com/yoismail)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-yomi--ismail-blue?logo=linkedin)](https://www.linkedin.com/in/yomi-ismail/)

---
