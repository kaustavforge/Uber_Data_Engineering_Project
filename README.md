# Uber data engineering project - Kafka + MinIO + Databricks Free Edition

Adapted from Ansh Lamba's Azure version. Catalog `uber` with four schemas (`raw`, `bronze`, `silver`, `gold`).
Every table is written with its full 3-part name in the code (e.g. `uber.silver.stg_rides`), so you never
choose a target per layer in the pipeline UI and there are no pipeline config keys.

```
FastAPI app ("Book a Ride") --> Kafka (ubertopic) --> consumer --> MinIO (NDJSON files)
                                                                      |  sync script / manual upload
                                                                      v
   Databricks Volume --> rides_raw --> stg_rides --> silver_obt --> dim_* + fact
   (raw)                   (bronze)      (silver)      (silver)       (gold)
```

| Ansh (Azure)                 | This project                        |
|------------------------------|-------------------------------------|
| Event Hubs                   | Kafka (Docker, KRaft)               |
| ADLS / Blob + SAS URLs       | MinIO (pgsty/minio) + UC Volume     |
| ADF                          | `sync_to_databricks.py` or manual   |
| Kafka reader in `ingest.py`  | Auto Loader on the Volume           |

| Schema | Tables |
|---|---|
| `uber.raw` | Volume `landing` (files only): `static/` (map_*.json, bulk_rides.json) and `rides_stream/` (live ride files) |
| `uber.bronze` | map_cities, map_cancellation_reasons, map_payment_methods, map_ride_statuses, map_vehicle_makes, map_vehicle_types, bulk_rides, rides_raw |
| `uber.silver` | stg_rides, silver_obt |
| `uber.gold` | dim_passenger, dim_driver, dim_vehicle, dim_payment, dim_booking, dim_location (SCD2), fact |

---

# Build order: when to code, when to run

Work top to bottom. Each phase = **code these files -> run this -> check this**. Don't start the next
phase until the check passes. Files are already in this repo; if you are re-typing it yourself to learn,
write them in this order.

## Phase 0 - Project setup (once)
**Code:** nothing. `pyproject.toml` and `uv.lock` already define the dependencies.
**Run:**
```bash
uv sync                          # creates .venv from pyproject.toml / uv.lock
cp .env.example .env             # Windows: copy .env.example .env
```
**Check:** `uv run python -c "import confluent_kafka, minio, fastapi"` prints nothing (no error).

## Phase 1 - Infrastructure: Kafka + MinIO
**Code:** `docker-compose.yml` (Kafka KRaft + `pgsty/minio`, the maintained fork of the discontinued `minio/minio`).
**Run:**
```bash
docker compose up -d
docker ps                        # kafka and minio both "Up"
```
**Check:** MinIO console opens at http://localhost:9001 (minioadmin / minioadmin).

## Phase 2 - Producer side: FastAPI books rides into Kafka (same idea as Ansh)
**Code, in this order:**
1. `app/data.py` - generates one fake ride (his generator, only the Azure import removed)
2. `app/producer.py` - `send_to_kafka()`; replaces his `connection.py` / Event Hubs client
3. `app/api.py` + `app/templates/*.html` - `/` shows the home page, `/book` generates a ride, sends it to
   Kafka and shows "Ride Confirmed"

**Run (terminal 2):**
```bash
uv run python app/api.py         # open http://localhost:8000 and click "Book a Ride" a few times
```
**Check (terminal 1) - read the message back from Kafka:**
```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ubertopic --from-beginning --max-messages 1
```
You should see one JSON ride.

*Optional shortcut:* clicking 200 times is boring, so `app/simulate.py` sends rides in a loop:
```bash
uv run python app/simulate.py --n 200 --delay 0.2
```
`--n` = how many rides to send. `--delay` = seconds to wait between rides. There is nothing special about
200 or 0.2: 200 x 0.2s is about 40 seconds of traffic, which makes the consumer in Phase 3 write 2-4 files
(it writes when it has 50 events or 20 seconds have passed) instead of one big file or 200 tiny ones.
Use smaller numbers if you like; `--delay 0` sends as fast as possible.

## Phase 3 - Kafka -> MinIO
**Code:** `pipeline/consumer_to_minio.py` - reads the topic, batches events, writes one NDJSON file
(one JSON ride per line) to bucket `uber-raw`, folder `rides_stream/`. Offsets are committed only
after the upload succeeds.
**Run (terminal 3, leave running):**
```bash
uv run python pipeline/consumer_to_minio.py
```
**Check:** MinIO console -> bucket `uber-raw` -> `rides_stream/` has files. Ctrl+C flushes the last batch.

## Phase 4 - Databricks: setup + static bronze
**Code:** `databricks/01_bronze_static.py` (notebook).
**Run, in this order:**
1. Import it as a notebook, run the first 2 code cells (creates `uber`, schemas `raw`/`bronze`/`silver`/`gold`,
   volume `uber.raw.landing`, folders `static/` and `rides_stream/`).
2. Put the static files (`data/*.json`) in the Volume `uber/raw/landing/static/` - choose one:
   - **GitHub (like Ansh's flow, tested: your workspace can reach raw.githubusercontent.com):** push this
     project to a **public** GitHub repo, set `GITHUB_RAW_BASE` in the notebook's optional download cell to
     `https://raw.githubusercontent.com/<you>/<repo>/main/data`, and run it. Use YOUR repo, not Ansh's:
     his `map_cities.json` has no `updated_at` column, which the SCD2 `dim_location` needs.
   - Script: fill DATABRICKS_* in `.env`, then
     `uv run python pipeline/sync_to_databricks.py --static`
   - Manual: Catalog -> uber -> raw -> landing -> Upload to this volume
3. Run the rest of the notebook (loads the 6 `map_*` tables and `bulk_rides`).

**Check:** `SELECT count(*) FROM uber.bronze.bulk_rides` returns 2000.

## Phase 5 - Move the live files into Databricks
**Code:** `pipeline/sync_to_databricks.py` (already used above). It copies new files from MinIO into the
Databricks Volume and remembers what it already sent (`.synced.json`).

**Before the first run:** create a token in Databricks (User Settings -> Developer -> Access tokens; the
default lifetime is 14 days, so you will have to renew it) and put it in `.env` as `DATABRICKS_TOKEN`,
together with `DATABRICKS_HOST`. If your workspace doesn't allow tokens, upload the MinIO files into
`rides_stream/` by hand instead (Catalog -> uber -> raw -> landing -> Upload to this volume).

**Run - pick one mode:**
```bash
uv run python pipeline/sync_to_databricks.py                       # once: sync new files, then stop
uv run python pipeline/sync_to_databricks.py --watch               # keep running, check MinIO every 30s
uv run python pipeline/sync_to_databricks.py --watch --interval 10 # check every 10s
```
`--watch` is the automatic mode. It stays running in its own terminal, checks MinIO every N seconds,
uploads any new file, and stops when you press Ctrl+C. If MinIO or Databricks has a hiccup it prints the
error and tries again on the next round. With the consumer and `--watch` both running, a ride reaches the
Volume about a minute after you book it, with no commands from you. The pipeline still has to run (Phase 7)
before it shows up in the tables.

**Check:** Volume `uber.raw.landing/rides_stream/` has at least one file. **Do this before the first
pipeline run**, otherwise Auto Loader has nothing to read.

## Phase 6 - Pipeline code (Jobs & Pipelines -> ETL pipeline)
**Code, in this order (this is the same order the data flows):**
1. `databricks/ingest.py` -> `uber.bronze.rides_raw` (Auto Loader; replaces his Event Hubs read)
2. `databricks/silver.py` -> `uber.silver.stg_rides` (two append flows: bulk + stream = "one big stream")
3. `databricks/silver_obt.sql` -> `uber.silver.silver_obt` (joins in all the map tables)
4. `databricks/model.py` -> `uber.gold.dim_*` and `uber.gold.fact` (AUTO CDC, dim_location is SCD2)

**Run:** create the pipeline, add those 4 files as source code, press Run. The UI may ask for a default
catalog/schema: pick `uber` / `bronze` once; it doesn't matter after that because every dataset name in
the code is fully qualified.
**Check:**
```sql
SELECT count(*) FROM uber.silver.stg_rides;      -- 2000 bulk + the streamed events
SELECT * FROM uber.silver.silver_obt LIMIT 10;
SELECT * FROM uber.gold.fact LIMIT 10;
```

## Phase 6b - Homework from the video: generate `silver_obt.sql` with Jinja (optional)
Ansh's homework is to stop copy-pasting the rendered Jinja output and write it into the `.sql` file
automatically. `tools/render_silver_obt.py` does that:
- `JINJA_CONFIG` = one entry per table (join, select columns, optional where), like his `silver_obt.ipynb`
- the Jinja template loops over it (`for`, `if loop.first / loop.last`, variables) and builds the
  `CREATE OR REFRESH STREAMING TABLE ... SELECT ... LEFT JOIN ...` statement
- the script writes the result straight into `databricks/silver_obt.sql`

**Code:** `tools/render_silver_obt.py`
**Run (locally, any time you change the config):**
```bash
uv run python tools/render_silver_obt.py
```
**Check:** `databricks/silver_obt.sql` is rewritten. The output is the same query as the hand-written
version (6 LEFT JOINs); then re-upload that file as the pipeline source.

*Inside Databricks instead (like him):* build the same `rendered` string in a notebook and write it with
`Path("/Workspace/Users/<you>/.../silver_obt.sql").write_text(rendered)` (Python `os`/`pathlib`).
I haven't tested writing to workspace paths on Free Edition.

## Phase 7 - Orchestrate
Jobs & Pipelines -> Create job -> task type "Pipeline" -> pick the pipeline -> add a schedule.

## The day-to-day loop (after everything is built)
Keep these running, one terminal each:
1. Terminal 1: `docker compose up -d` (once, then it keeps running in Docker)
2. Terminal 2: `uv run python app/api.py` -> book rides in the browser
3. Terminal 3: `uv run python pipeline/consumer_to_minio.py` -> files appear in MinIO
4. Terminal 4: `uv run python pipeline/sync_to_databricks.py --watch` -> files appear in the Volume
5. Databricks: run the pipeline (or let the Job do it on its schedule) -> new rides flow to silver and gold

Steps 4-5 are the hops that exist only because Free Edition can't reach Kafka or MinIO on your laptop.

---

## Differences from Ansh's repo (on purpose)
- `ingest.py`: Auto Loader instead of Event Hubs (same `rides` column out)
- `map_cities.json`: added `updated_at` (his SQL needs it for SCD2), fixed "Los Angelas"
- `model.py`: `sequence_by` is a timestamp instead of the id column; dim_location is a view like the others;
  duplicate line removed; all names fully qualified
- `bulk_rides` read with an explicit schema so both append flows have identical column types
- `data.py` is his generator unchanged (new passenger/driver/vehicle id per ride), only the Azure import removed

## README honesty note
Kafka and MinIO are local stand-ins for Event Hubs and ADLS. Events reach Databricks as micro-batch files,
so this is near-real-time, not true streaming end to end.
