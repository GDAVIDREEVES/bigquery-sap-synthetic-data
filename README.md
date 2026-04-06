# ACDOCA Synthetic Data Generator

Synthetic SAP S/4HANA **ACDOCA** (Universal Journal) data for **Databricks** (Unity Catalog, Delta Lake), **Google BigQuery** (via the Spark BigQuery connector), and local Spark. Use it for transfer-pricing analysis, intercompany testing, financial supply-chain modeling, Pillar Two / GloBE demos, and ML pipelines without production SAP extracts.

Design and column coverage follow [SPEC-ACDOCA-Synthetic-Generator.md](SPEC-ACDOCA-Synthetic-Generator.md).

## Features

- **538-column schema** aligned with the spec appendix (sanitized names, e.g. `.INCLU-_PN` → `INCLU_PN`).
- **Industry templates** drive domestic **GL mix**, **posting period seasonality**, default **intercompany share**, and optional **entity-role** hints per country.
- **Streamlit UI** (`acdoca_generator/app.py`) with **named demo presets**, custom parameters, and **strict vs fast** validation.
- **Financial supply chain** (optional): multi-hop flows with materials, TP method, markups, plants, and linked IC postings; preset `supply_chain_demo`. **`generate_acdoca_dataframe`** returns **`GenerationResult`** (`acdoca_df`, optional **`supply_chain_flows_df`**). **Dash + Cytoscape** viewer: `pip install -e ".[viz]"` then run **`python -m acdoca_generator.dash_app.app --data flows.json`** in a terminal and keep it open while you browse **http://127.0.0.1:8050/** (works with Dash 2 and Dash 3; see `requirements-viz.txt`).
- **Spark generators**: master data, domestic transactions, intercompany pairs, amounts/currency, closing patterns, document numbering.
- **Fast path (optional)**: **`acdoca_generator/core/`** implements domestic journal lines in **Polars** (no JVM) for quick generation and tests. Row-level hashes differ from Spark SQL `hash()`; use **`acdoca_generator.spark_bridge.polars_to_spark`** to materialize a Polars frame as a Spark `DataFrame` in one step when you need Spark or Delta/BQ writes. Install with **`pip install -e ".[fast]"`** (or **`.[dev,fast]`** for tests).
- **Validators**: debit/credit balance and IC reconciliation checks (full PK uniqueness scan optional).
- **Delta writer** with schema enforcement for catalog-backed tables; `generator.version` is filled from the installed **package version** when not set explicitly.
- **BigQuery writer** (Spark connector): time partitioning on `BUDAT` (MONTH), clustering on `RBUKRS`, `GJAHR`, `POPER`; generator metadata as **table labels** (`bigQueryTableLabel.*`).

## Project layout

| Path | Role |
|------|------|
| `acdoca_generator/app.py` | Streamlit entry point |
| `acdoca_generator/config/` | Industries, presets, countries, chart of accounts, field tiers, operating models |
| `acdoca_generator/generators/` | Pipeline, master data, transactions, intercompany, supply chain, amounts, closing, document |
| `acdoca_generator/core/` | Polars-based company master + domestic generator (Spark-free fast path) |
| `acdoca_generator/spark_bridge.py` | `polars_to_spark(session, polars_df)` for a single conversion hop |
| `acdoca_generator/dash_app/` | Optional Dash app: interactive supply-chain network graph |
| `requirements-viz.txt` | Optional deps for the Dash viewer (`dash`, `plotly`, `dash-cytoscape`, `pandas`) |
| `acdoca_generator/validators/` | Balance and consistency checks |
| `acdoca_generator/utils/` | Spark schema and Delta / Parquet / BigQuery writer |
| `notebooks/` | Databricks + BigQuery setup SQL and parameterized generation notebooks |
| `scripts/run_generate_bq.py` | CLI: generate and write to BigQuery (Spark + connector) |
| `databricks.yml` | Optional Databricks Asset Bundle (sample job) |
| `.github/workflows/ci.yml` | PR/push CI: Python only, **`pytest -m "not spark"`** (fast; Polars + non-Spark tests) |
| `.github/workflows/ci-spark.yml` | Optional Spark suite: **`pytest -m spark`** (Temurin 17); `workflow_dispatch` + weekly schedule |
| `scripts/complete_github_ssh.sh` | After registering your SSH key on GitHub, run to verify `ssh` and push `main` |

## Requirements

- Python **3.10+**
- **PySpark** 3.5.x (Spark 3.5 compatible)
- **Java** (for full **Spark** tests, local Spark generation, Databricks-style runs, and BigQuery via the connector). The default **PR CI** job does **not** require Java; it runs tests marked **`not spark`** only.
- For production-style runs: a Spark session that can write to **Unity Catalog** Delta tables (e.g. Databricks cluster / DBR), or **BigQuery** using the Spark BigQuery connector plus a **GCS bucket** for staging. Spark-backed tests use `local[2]` when Java is available.

## Install

```bash
git clone https://github.com/GDAVIDREEVES/bigquery-sap-synthetic-data.git
cd bigquery-sap-synthetic-data
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

Install **test** and **fast** (Polars) dependencies (pytest is not in `requirements.txt`):

```bash
pip install -e ".[dev,fast]"
```

For tests only without Polars: `pip install -e ".[dev]"` (then run `pytest -m spark` or a subset; **`not spark`** tests that import Polars will fail without **`[fast]`**).

## Tests

```bash
pip install -e ".[dev,fast]"
pytest -m "not spark"   # default PR-style: seconds, no Java
```

**Spark integration tests** (full PySpark pipeline, bridge tests):

```bash
# Requires JDK 17+ and PySpark; set on flaky runners if needed:
export SPARK_LOCAL_IP=127.0.0.1
pytest -m spark -vv
```

Tests that use the **`spark`** session fixture or shared Spark DataFrame fixtures are marked **`@pytest.mark.spark`** automatically via [`acdoca_generator/tests/conftest.py`](acdoca_generator/tests/conftest.py).

**Java:** Only required for **`pytest -m spark`**. Without Java, run **`pytest -m "not spark"`** only. Install **Temurin 17** (or JDK 11+); on macOS, `brew install --cask temurin@17` and set `JAVA_HOME` if needed (`/usr/libexec/java_home -v 17`).

**Faster local runs:** Prefer **`pytest -m "not spark"`** for routine checks. A full Spark run can take many minutes on small machines.

**Supply chain Spark test:** `test_supply_chain_generates_hops_and_sc_awref` is skipped unless you set `ACDOCA_RUN_SPARK_TESTS=1` (slow; requires Java).

**CI**

- [`.github/workflows/ci.yml`](.github/workflows/ci.yml): on push/PR to **`main`**, Python 3.11, **`pip install -e ".[dev,fast]"`**, then **`pytest -m "not spark"`** (no JDK step).
- [`.github/workflows/ci-spark.yml`](.github/workflows/ci-spark.yml): **Temurin 17**, same install, **`pytest -m spark`** — trigger manually (**Actions → CI Spark integration → Run workflow**) or on the weekly schedule.

**Performance note:** The Spark pipeline is optimized for **distributed** runs; on tiny local/CI data volumes, **JVM and job startup** dominate. The Polars **`core`** path is intended for fast iteration; production writes to Delta/BigQuery still use Spark as today.

## Databricks (recommended)

This repo includes Databricks-ready notebooks and an optional Databricks Asset Bundle job definition.

### 1) Create the Unity Catalog schema

Run:

- `notebooks/00_uc_setup.sql`

It creates `synthetic` + `synthetic.acdoca` (adjust to your naming / permissions model).

### 2) Run the generator notebook

Run:

- `notebooks/01_generate_acdoca.py`

Parameters via `dbutils.widgets` (or Job base parameters):

| Parameter | Notes |
|-----------|--------|
| `preset` | `custom` (default) or `quick_smoke`, `tp_workshop`, `globe_lite`, `ml_features`. When not `custom`, preset overrides the row below. |
| `validation_profile` | `strict` (default) or `fast`. Fast skips the expensive full-table **PK uniqueness** check (demo-only trade-off). |
| `industry_key` | Canonical keys: `pharmaceutical`, `medical_device`, `consumer_goods`, `technology`, `media`. Legacy alias: `consumer_products` maps to `consumer_goods`. |
| `country_isos_csv` | e.g. `US,DE,GB` |
| `fiscal_year` | e.g. `2026` |
| `fiscal_variant` | `calendar` or `april` |
| `complexity` | `light`, `medium`, `high`, `very_high` |
| `txn_per_cc_per_period` | e.g. `1000` |
| `ic_pct` | Fraction \(0..1\). **Empty string** uses the selected industry template `ic_share_default` (custom mode only). |
| `include_reversals` / `include_closing` | `true` / `false` |
| `seed` | e.g. `42` |
| `full_table_name` | e.g. `synthetic.acdoca.journal_entries` |
| `output_format` | `delta` or `parquet`; optional `parquet_path` |

**About a five-minute demo:** set `preset` to `quick_smoke` (light volume, fast validation, no closing entries) or tune `txn_per_cc_per_period` down and use `validation_profile`=`fast`.

### 3) Optional: deploy as a job with a Databricks Asset Bundle

This repo includes `databricks.yml` with a sample job. To use it:

- Set `DATABRICKS_HOST` and `DATABRICKS_NODE_TYPE_ID`
- Use the Databricks CLI bundle workflow (e.g. `databricks bundle deploy`, `databricks bundle run`)

The sample job cluster uses **Photon** (`runtime_engine: PHOTON`) and **autoscaling** (`min_workers` 1, `max_workers` 4). If your workspace does not support Photon, remove `runtime_engine` from `databricks.yml` or set a cluster policy that fixes compatible settings.

Optional: attach a **cluster policy** in the Databricks UI or extend the bundle with a non-empty `policy_id` on `new_cluster` if your organization standardizes node types and Spark config that way.

## Google BigQuery (PySpark + connector)

Generation logic is unchanged; output goes to BigQuery using the [Spark BigQuery connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) (default Maven coordinate: `com.google.cloud.spark:spark-3.5-bigquery:0.44.1`, overridable with `ACDOCA_SPARK_BQ_PACKAGE`).

### Prerequisites

- GCP project with **BigQuery** and **Cloud Storage** enabled.
- A **GCS bucket** used only (or shared) for connector temporary files during load (`temporaryGcsBucket`).
- **Credentials**: workload identity on Dataproc, or `GOOGLE_APPLICATION_CREDENTIALS` for local runs.
- **IAM**: the Spark driver’s identity (e.g. Dataproc cluster service account) needs roles such as **BigQuery Data Editor** on the target dataset (or project) and **Storage Object Admin** (or create/use) on the staging bucket.

### 1) Create the BigQuery dataset

Edit and run:

- [`notebooks/00_bq_setup.sql`](notebooks/00_bq_setup.sql)

Replace `MY_PROJECT` with your project id. The first write can also **create** the table (`CREATE_IF_NEEDED`); the writer sets **partitioning** (field `BUDAT`, type **MONTH**) and **clustering** (`RBUKRS`, `GJAHR`, `POPER`).

### 2) Run from the CLI (any Spark 3.5 + connector)

From the repo root, with Python 3.10+ and a JDK:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json   # if not on GCP with default ADC
export ACDOCA_GCS_TEMP_BUCKET=your-staging-bucket
python scripts/run_generate_bq.py \
  --full-table-name YOUR_PROJECT.synthetic_acdoca.journal_entries \
  --preset quick_smoke
```

Use `--help` for all flags. Parameters match the Databricks notebook semantics (see table below). Override the connector package with `ACDOCA_SPARK_BQ_PACKAGE` if your Spark version differs.

### 3) Notebook on Dataproc / Vertex Workbench

- [`notebooks/01_generate_acdoca_bq.py`](notebooks/01_generate_acdoca_bq.py) mirrors [`notebooks/01_generate_acdoca.py`](notebooks/01_generate_acdoca.py) but writes with `output_format=bigquery`. Ensure the cluster or session has the Spark BigQuery connector JAR (e.g. Dataproc image that supports `spark-3.5-bigquery`, or `spark.jars.packages` as in `scripts/run_generate_bq.py`).
- Widgets / parameters: same as Databricks, plus:
  - `full_table_name`: **`project.dataset.table`** (not Unity Catalog).
  - `gcs_temp_bucket`: staging bucket name, or rely on `ACDOCA_GCS_TEMP_BUCKET`.

### Parameter parity (Databricks vs BigQuery)

| Parameter | Databricks notebook | BigQuery notebook / CLI |
|-----------|---------------------|-------------------------|
| `preset`, `validation_profile`, `industry_key`, `country_isos_csv`, `fiscal_year`, `fiscal_variant`, `complexity`, `txn_per_cc_per_period`, `ic_pct`, `include_reversals`, `include_closing`, `seed` | same | same (CLI: `--ic-pct`, `--country-isos`, etc.) |
| `full_table_name` | `catalog.schema.table` | `project.dataset.table` |
| `output_format` | `delta` / `parquet` | N/A (always BigQuery in BQ entrypoints) |
| `gcs_temp_bucket` | — | Required (widget, `ACDOCA_GCS_TEMP_BUCKET`, or `--gcs-temp-bucket`) |

### Dataproc / `spark-submit`

`scripts/run_generate_bq.py` configures `spark.jars.packages` for a local `SparkSession`. On **Dataproc**, you can instead submit it as a **PySpark** job: install this package on the cluster (initialization action or custom image with `pip install`), attach the BigQuery connector (e.g. `gs://spark-lib/bigquery/spark-3.5-bigquery-0.44.1.jar` or equivalent `--packages` on `spark-submit`), and pass the same CLI arguments after `--`. If you use plain `spark-submit` with `--packages com.google.cloud.spark:spark-3.5-bigquery:0.44.1`, ensure driver and executors can import `acdoca_generator` (zip the repo with `pip wheel` / `venv` layout as your platform requires).

## Streamlit

```bash
streamlit run acdoca_generator/app.py
```

Choose **delta**, **parquet**, or **bigquery** in the UI. For BigQuery, provide **`project.dataset.table`** and a **GCS staging bucket**; the app restarts the local Spark session with `spark.jars.packages` set to load the BigQuery connector (not used when `DATABRICKS_HOST` is set—install the connector on the Databricks cluster instead). Optional env defaults: `ACDOCA_BQ_TABLE`, `ACDOCA_GCS_TEMP_BUCKET`, `ACDOCA_SPARK_BQ_PACKAGE`. See [`acdoca_generator/utils/spark_writer.py`](acdoca_generator/utils/spark_writer.py) for write paths and options.

## Git and GitHub (SSH)

This repo uses an SSH remote (`git@github.com:...`). On macOS, generate a key (`ssh-keygen -t ed25519`), add `~/.ssh/id_ed25519.pub` under **GitHub → Settings → SSH and GPG keys**, then confirm with `ssh -T git@github.com`.

Cursor uses your system Git and `~/.ssh` (same as Terminal). After SSH works, you can push from Cursor or run:

```bash
./scripts/complete_github_ssh.sh
```

## License

No license file is included in this repository; add one if you intend to redistribute or contribute under explicit terms.
