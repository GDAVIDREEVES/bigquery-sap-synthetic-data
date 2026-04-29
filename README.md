# ACDOCA Synthetic Data Generator

Synthetic SAP S/4HANA **ACDOCA** (Universal Journal) data for **Google BigQuery** (via the Spark BigQuery connector), **local PySpark** (Delta Lake, Parquet), and an optional **Polars** fast path for domestic-only generation. Use it for transfer-pricing analysis, intercompany testing, financial supply-chain modeling, Pillar Two / GloBE demos, and ML pipelines — without production SAP extracts.

Design and column coverage follow [SPEC-ACDOCA-Synthetic-Generator.md](SPEC-ACDOCA-Synthetic-Generator.md). For step-by-step recipes — first-time setup, the **BigQuery Studio Jupyter notebook** (the supported run path), CLI → BigQuery, Dash viewer, tests, and troubleshooting — see [README-WORKFLOW.md](README-WORKFLOW.md).

## Contents

- [Highlights](#highlights)
- [Install](#install)
- [Run it: BigQuery Studio Jupyter notebook](#run-it-bigquery-studio-jupyter-notebook)
- [Programmatic API](#programmatic-api)
- [Generate to BigQuery (CLI)](#generate-to-bigquery-cli)
- [Tests and CI](#tests-and-ci)
- [Local Spark gotchas](#local-spark-gotchas)
- [Project layout](#project-layout)
- [Requirements](#requirements)
- [License](#license)

## Highlights

- **538-column ACDOCA schema** aligned with the spec appendix (sanitized names, e.g. `.INCLU-_PN` → `INCLU_PN`).
- **Industry templates** drive domestic GL mix, posting-period seasonality, default intercompany share, and optional entity-role hints per country.
- **BigQuery Studio Jupyter notebook** ([`notebooks/02_generate_acdoca_bq_studio.ipynb`](notebooks/02_generate_acdoca_bq_studio.ipynb)) is the supported run path: an `ipywidgets` form with named demo presets, custom parameters, and strict-vs-fast validation, running inside the BigQuery console with Spark + the BQ connector pre-wired.
- **TP role taxonomy** (20 entity roles): manufacturing (`TOLL`, `CMFR`, `LMFR`, `FRMF`, `IPLIC`); distribution (`LRD`, `FFD`, `COMM`, `COMA`, `BSDIST`); services (`RDSC`, `RSP`, `SSP`, `SSC`); IP & principal (`IPDEV`, `IPPR`, `ENTR`); hybrid (`RHQ`, `CPE`, `FINC`). Each role carries an operating-margin band and typical-country list — see [`config/operating_models.py`](acdoca_generator/config/operating_models.py).
- **Functional area (RFAREA)** populated per line, role-conditioned: e.g. LRD payroll → `0200` (Sales), IPPR payroll → `0300` (Admin), TOLL payroll → `0100` (Production), RDSC payroll → `0400` (R&D). Drives the segment-level P&L below — see [`config/functional_areas.py`](acdoca_generator/config/functional_areas.py).
- **Financial supply chain** (optional, preset `supply_chain_demo`): multi-hop flows with materials, TP method, markups, plants, and linked IC postings. Multi-template per industry, causal POPER ordering within a chain, optional `fanout_all` for one-to-many hops, triangular markup distribution, and per-step `tp_method_key` override.
- **Segment-level P&L** (optional, opt-in via `include_segment_pl=True`): `build_segment_pl(acdoca_df, companies_df)` aggregates to `(RBUKRS × ROLE_CODE × SEGMENT × GJAHR × POPER)` with `revenue / cogs / opex_production / opex_rd / opex_sm / opex_ga / opex_dist / ic_charges / depreciation / operating_profit / operating_margin`. Opex split is driven by RFAREA — purpose-built for TP diagnostics (LRD margin testing, principal residual, routine-return checks). See [`aggregations/segment_pl.py`](acdoca_generator/aggregations/segment_pl.py).
- **Dash + Cytoscape viewer** for supply-chain graphs: `pip install -e ".[viz]"`, then `python -m acdoca_generator.dash_app.app --data flows.json` and browse [http://127.0.0.1:8050](http://127.0.0.1:8050) (Dash 2 and 3 supported).
- **Spark generators** for master data, domestic transactions, intercompany pairs, amounts/currency, closing patterns, and document numbering.
- **Polars fast path** ([`acdoca_generator/core/`](acdoca_generator/core/)) for domestic journal lines without a JVM — quick generation and tests. Row-level hashes differ from Spark SQL `hash()`; use `acdoca_generator.spark_bridge.polars_to_spark` to materialize a Polars frame as a Spark `DataFrame` in one step when you need Spark or Delta/BQ writes. Install with `pip install -e ".[fast]"` (or `.[dev,fast]` for tests).
- **Validators**: debit/credit balance and IC reconciliation checks (full PK uniqueness scan optional).
- **Writers**: Delta/Parquet (Spark, schema-enforced); BigQuery (Spark connector) with `BUDAT` MONTH partitioning, clustering on `RBUKRS / GJAHR / POPER`, and generator metadata as table labels (`bigQueryTableLabel.*`). `generator.version` is filled from the installed package version when not set explicitly.

## Install

For the Jupyter notebook path ([next section](#run-it-bigquery-studio-jupyter-notebook)) the BigQuery Studio runtime installs the package itself — you don't need to install anything locally.

For the CLI, programmatic API, and tests:

```bash
git clone https://github.com/GDAVIDREEVES/bigquery-sap-synthetic-data.git
cd bigquery-sap-synthetic-data
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e ".[dev,fast]"   # tests + Polars fast path
```

Optional extras: `.[viz]` for the Dash supply-chain viewer; `.[dev]` alone for tests without Polars (note: `not spark` tests that import Polars will fail without `[fast]`).

## Run it: BigQuery Studio Jupyter notebook

The supported, recommended run path. The notebook is [`notebooks/02_generate_acdoca_bq_studio.ipynb`](notebooks/02_generate_acdoca_bq_studio.ipynb). It clones this repo into the BigQuery Studio runtime, installs `acdoca_generator` in editable mode, and renders an `ipywidgets` form (preset dropdown, industry, country multi-select, complexity, sliders for txn-per-CC-per-period / IC% / SC chain count / challenged-share, checkboxes for supply-chain / segment-PL / year-end trueup, Generate button). Clicking Generate writes to the configured BigQuery table.

You authenticate as your Google identity (no service-account JSON), and the runtime ships with Spark + the BigQuery connector pre-wired — no `spark.jars.packages`, no GCS Hadoop connector JAR to manage, no local Java install.

Setup, in order:

1. Run [`notebooks/00_bq_setup.sql`](notebooks/00_bq_setup.sql) (replace `MY_PROJECT`) to create the dataset.
2. Create a GCS staging bucket (`gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET`) and grant your user `roles/storage.objectAdmin`.
3. Open BigQuery Studio → **+ Add → Python notebook**, pick the **PySpark** runtime.
4. Import [`notebooks/02_generate_acdoca_bq_studio.ipynb`](notebooks/02_generate_acdoca_bq_studio.ipynb) (or paste it into a new notebook), edit `PROJECT_ID` / `BQ_DATASET` / `GCS_BUCKET` in cell 2, and run cells top-to-bottom.
5. In the rendered form, pick a preset (start with `quick_smoke` to confirm wiring), tick the realism extras you want, and click **Generate**.

The notebook runs the same `generate_acdoca_dataframe` + `write_acdoca_table` code path as `scripts/run_generate_bq.py`. See [README-WORKFLOW.md → Workflow A](README-WORKFLOW.md#a-bigquery-studio-jupyter-notebook-primary) for the full step-by-step including verification queries and iteration patterns.

## Programmatic API

Generate a small dataset with supply chain + segment P&L from Python (used by integration tests and downstream code, not for ad-hoc runs):

```python
from pyspark.sql import SparkSession
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe

spark = SparkSession.builder.master("local[2]").getOrCreate()
cfg = GenerationConfig(
    industry_key="pharmaceutical",
    country_isos=["US", "DE", "IE", "CH"],
    fiscal_year=2025,
    fiscal_variant="calendar",
    complexity="medium",
    txn_per_cc_per_period=2,
    include_reversals=False,
    include_closing=False,
    seed=42,
    include_supply_chain=True,    # multi-hop IC + flows DataFrame
    sc_chains_per_period=12,
    include_segment_pl=True,      # functional-opex P&L by entity × role × period
)
result = generate_acdoca_dataframe(spark, cfg)
result.acdoca_df              # 538-column ACDOCA journal lines
result.supply_chain_flows_df  # one row per hop (chain, step, TP method, markup, parties)
result.segment_pl_df          # P&L: revenue, cogs, opex_rd/sm/ga/dist/production, OP, OM
```

`generate_acdoca_dataframe` returns a `GenerationResult` (`acdoca_df`, optional `supply_chain_flows_df`, optional `segment_pl_df`).

A ready-to-run smoke script lives at [`scripts/dev_smoke_supply_chain.py`](scripts/dev_smoke_supply_chain.py); it prints the segment P&L, asserts IC document balance, and exports `flows.json` for the Dash viewer.

## Generate to BigQuery (CLI)

The CLI is the high-volume / scripted alternative to the Jupyter notebook. Use it for `ml_features`, ~1M-row custom configs, and CI canaries. Output goes to BigQuery via the [Spark BigQuery connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) (default Maven coordinate `com.google.cloud.spark:spark-3.5-bigquery:0.44.1`, overridable with `ACDOCA_SPARK_BQ_PACKAGE`).

### Prerequisites

- GCP project with **BigQuery** and **Cloud Storage** enabled.
- A **GCS bucket** used (or shared) for connector temp files during load (`temporaryGcsBucket`).
- **Credentials**: workload identity on Dataproc, or `GOOGLE_APPLICATION_CREDENTIALS` for local runs.
- **IAM**: the Spark driver's identity needs **BigQuery Data Editor** on the target dataset (or project) and **Storage Object Admin** (or create/use) on the staging bucket.

### GCS Hadoop connector (local-mode Spark)

The Spark BigQuery connector stages temp files via `gs://`, which needs the GCS Hadoop FileSystem on the classpath. Dataproc bundles this; for local-mode Spark (laptop, Colab Enterprise default runtime), download the **shaded** GCS connector JAR once:

```bash
mkdir -p ~/.spark-jars
curl -L -o ~/.spark-jars/gcs-connector-hadoop3-2.2.21-shaded.jar \
  https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.21/gcs-connector-hadoop3-2.2.21-shaded.jar
```

[`scripts/run_generate_bq.py`](scripts/run_generate_bq.py) auto-detects this JAR at `~/.spark-jars/gcs-connector-hadoop3-2.2.21-shaded.jar` (overridable via `ACDOCA_SPARK_GCS_JAR`). Without it, BigQuery writes fail with `UnsupportedFileSystemException: No FileSystem for scheme "gs"`. The shaded variant is preferred over `spark.jars.packages` because the unshaded gcs-connector pulls a fragile transitive tree from Maven Central.

### 1. Create the BigQuery dataset

Edit and run [`notebooks/00_bq_setup.sql`](notebooks/00_bq_setup.sql) (replace `MY_PROJECT`). The first write can also create the table (`CREATE_IF_NEEDED`); the writer sets partitioning (field `BUDAT`, type MONTH) and clustering (`RBUKRS`, `GJAHR`, `POPER`).

### 2. Run from the CLI

From the repo root, with Python 3.10+ and a JDK:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json   # if not on GCP with default ADC
export ACDOCA_GCS_TEMP_BUCKET=your-staging-bucket
python scripts/run_generate_bq.py \
  --full-table-name YOUR_PROJECT.synthetic_acdoca.journal_entries \
  --preset quick_smoke
```

Use `--help` for all flags. Parameters match the BigQuery Studio notebook form ([table below](#parameters-notebook--cli)). Override the connector package with `ACDOCA_SPARK_BQ_PACKAGE` if your Spark version differs.

#### Larger runs (`ml_features` and beyond)

The CLI is the most reliable path for high-volume runs — no form, but no JVM-heap ceiling beyond what your laptop has. Bump driver memory to suit your scale:

```bash
SPARK_DRIVER_MEMORY=8g python3 scripts/run_generate_bq.py \
  --preset ml_features \
  --full-table-name acdoca-synthetic-greg.synthetic_acdoca.journal_entries
```

For ~1M-row custom configs:

```bash
SPARK_DRIVER_MEMORY=8g python3 scripts/run_generate_bq.py \
  --preset custom \
  --industry-key pharmaceutical \
  --country-isos US,DE,CH,IE,IN,FR,GB,JP,IT,ES \
  --complexity very_high \
  --txn-per-cc-per-period 3500 \
  --ic-pct 0.35 \
  --sc-chains 100 \
  --full-table-name acdoca-synthetic-greg.synthetic_acdoca.journal_entries
```

For runtime-selection guidance (BQ Studio default vs Vertex AI Workbench vs laptop CLI), see [`docs/runtime-options.md`](docs/runtime-options.md).

### 3. Notebook on Dataproc / Vertex Workbench

[`notebooks/01_generate_acdoca_bq.py`](notebooks/01_generate_acdoca_bq.py) is a parameterized PySpark notebook (cells delimited by `# %%`) that writes with `output_format=bigquery`. Ensure the cluster or session has the Spark BigQuery connector JAR (e.g. a Dataproc image that supports `spark-3.5-bigquery`, or `spark.jars.packages` as in [`scripts/run_generate_bq.py`](scripts/run_generate_bq.py)). Parameters align with the BigQuery Studio notebook and CLI; for BigQuery specifically:

- `full_table_name`: `project.dataset.table` (BigQuery three-part name).
- `gcs_temp_bucket`: staging bucket name, or rely on `ACDOCA_GCS_TEMP_BUCKET`.

### Parameters (notebook / CLI)

| Parameter | BigQuery Studio notebook form | CLI (`run_generate_bq.py`) |
|-----------|-------------------------------|----------------------------|
| `preset`, `validation_profile`, `industry_key`, `country_isos_csv`, `fiscal_year`, `fiscal_variant`, `complexity`, `txn_per_cc_per_period`, `ic_pct`, `include_reversals`, `include_closing`, `seed` | widgets | `--ic-pct`, `--country-isos`, etc. |
| `full_table_name` | `PROJECT_ID.BQ_DATASET.BQ_TABLE` constants in cell 2 | `--full-table-name` |
| `output_format` | N/A (always BigQuery) | N/A |
| `gcs_temp_bucket` | `GCS_BUCKET` constant in cell 2 (or `ACDOCA_GCS_TEMP_BUCKET`) | `--gcs-temp-bucket` or env |

### Dataproc / `spark-submit`

[`scripts/run_generate_bq.py`](scripts/run_generate_bq.py) configures `spark.jars.packages` for a local `SparkSession`. On Dataproc, submit it as a PySpark job: install this package on the cluster (initialization action or custom image with `pip install`), attach the BigQuery connector (e.g. `gs://spark-lib/bigquery/spark-3.5-bigquery-0.44.1.jar`, or `--packages` on `spark-submit`), and pass the same CLI arguments after `--`. With plain `spark-submit --packages com.google.cloud.spark:spark-3.5-bigquery:0.44.1`, ensure driver and executors can import `acdoca_generator` (zip the repo with `pip wheel` / venv layout as your platform requires).

## Tests and CI

Default fast suite (seconds, no Java):

```bash
pip install -e ".[dev,fast]"
pytest -m "not spark"
```

Spark integration tests (full PySpark pipeline, bridge tests):

```bash
# Requires JDK 17+ and PySpark; set on flaky runners if needed:
export SPARK_LOCAL_IP=127.0.0.1
pytest -m spark -vv
```

Tests that use the `spark` session fixture or shared Spark DataFrame fixtures are auto-marked `@pytest.mark.spark` via [`acdoca_generator/tests/conftest.py`](acdoca_generator/tests/conftest.py).

The supply-chain Spark test `test_supply_chain_generates_hops_and_sc_awref` is skipped unless `ACDOCA_RUN_SPARK_TESTS=1` (slow; requires Java).

### Verifying the harness

Two reliability gates guard the larger presets:

```bash
# All 6 presets, full pipeline, no BigQuery write (~1 min on a laptop)
ACDOCA_RUN_SPARK_TESTS=1 SPARK_DRIVER_MEMORY=4g \
  pytest -m spark acdoca_generator/tests/test_presets_full_pipeline.py

# Real BigQuery write canary (writes ~30K rows, verifies via `bq query`, drops table)
ACDOCA_RUN_SPARK_TESTS=1 ACDOCA_RUN_BQ_TESTS=1 \
  ACDOCA_BQ_TABLE=your-project.your_dataset.canary \
  ACDOCA_GCS_TEMP_BUCKET=your-staging-bucket \
  pytest acdoca_generator/tests/test_bq_write_canary.py
```

Per-preset row counts (post-Phase-A reliability work, MacBook Pro local Spark, `seed=42`):

| Preset | Rows | Local elapsed |
|---|---|---|
| `quick_smoke` | 4,800 | ~6 s |
| `globe_lite` | 29,976 | ~14 s |
| `tp_workshop` | 73,144 | ~9 s |
| `supply_chain_demo` | 85,788 | ~10 s |
| `ml_features` | 97,084 | ~10 s |

### CI workflows

- [`.github/workflows/ci.yml`](.github/workflows/ci.yml): on push/PR to `main`, Python 3.11, `pip install -e ".[dev,fast]"`, then `pytest -m "not spark"` (no JDK step).
- [`.github/workflows/ci-spark.yml`](.github/workflows/ci-spark.yml): Temurin 17, same install, `pytest -m spark` — trigger manually (Actions → CI Spark integration → Run workflow) or on the weekly schedule.

The Spark pipeline is optimized for distributed runs; on tiny local/CI data volumes, JVM and job startup dominate. The Polars `core` path is intended for fast iteration; production writes to Delta/BigQuery still go through Spark.

## Local Spark gotchas

First-run Spark friction usually traces to one of these. Set them before any `pytest -m spark` or local generation:

```bash
# Default 1g driver heap OOMs even on tiny generations during shuffle.
export SPARK_DRIVER_MEMORY=4g

# Without this, workers spawn the system Python (often 3.9) and fail with
# PYTHON_VERSION_MISMATCH against the driver's venv interpreter.
export PYSPARK_PYTHON="$(which python)"

# Spark binds to hostname; resolution can be flaky on CI runners and laptops.
export SPARK_LOCAL_IP=127.0.0.1
```

**Python version:** the project supports Python 3.10+. The Py3.12 `toPandas`/`distutils` issue in the in-repo supply-chain JSON exporter is fixed (commit `10d642e`). The Spark 3.5 ecosystem is most stable on Python 3.11; if you hit obscure JVM-side errors on 3.12, try a 3.11 venv first.

**Java:** only required for `pytest -m spark`. Without Java, run `pytest -m "not spark"` only. Install Temurin 17 (or JDK 11+); on macOS, `brew install --cask temurin@17` and set `JAVA_HOME` if needed (`/usr/libexec/java_home -v 17`).

## Project layout

| Path | Role |
|------|------|
| [`notebooks/02_generate_acdoca_bq_studio.ipynb`](notebooks/02_generate_acdoca_bq_studio.ipynb) | Primary run path: BigQuery Studio Jupyter notebook with `ipywidgets` form |
| [`notebooks/00_bq_setup.sql`](notebooks/00_bq_setup.sql) | One-time BigQuery dataset DDL |
| [`notebooks/01_generate_acdoca_bq.py`](notebooks/01_generate_acdoca_bq.py) | Parameterized PySpark notebook for Dataproc / Vertex Workbench |
| [`acdoca_generator/config/`](acdoca_generator/config/) | Industries, presets, countries, chart of accounts, field tiers, operating models, functional areas, supply-chain templates, TP methods, materials |
| [`acdoca_generator/generators/`](acdoca_generator/generators/) | Pipeline, master data, transactions, intercompany, supply chain, amounts, closing, document |
| [`acdoca_generator/aggregations/`](acdoca_generator/aggregations/) | Segment-level P&L rollup (`build_segment_pl`); functional opex split driven by RFAREA |
| [`acdoca_generator/core/`](acdoca_generator/core/) | Polars-based company master + domestic generator (Spark-free fast path) |
| [`acdoca_generator/spark_bridge.py`](acdoca_generator/spark_bridge.py) | `polars_to_spark(session, polars_df)` for a single conversion hop |
| [`acdoca_generator/dash_app/`](acdoca_generator/dash_app/) | Optional Dash app: interactive supply-chain network graph |
| [`acdoca_generator/validators/`](acdoca_generator/validators/) | Balance and consistency checks |
| [`acdoca_generator/utils/`](acdoca_generator/utils/) | Spark schema and Delta / Parquet / BigQuery writer |
| [`scripts/run_generate_bq.py`](scripts/run_generate_bq.py) | CLI: generate and write to BigQuery (Spark + connector) |
| [`scripts/dev_smoke_supply_chain.py`](scripts/dev_smoke_supply_chain.py) | Local smoke runner: small generation with supply chain + segment P&L, exports flows JSON for the Dash viewer |
| [`diagnostics/`](diagnostics/) | Findings/notes from realism reviews (e.g. [`sc-realism-2026-04-27.md`](diagnostics/sc-realism-2026-04-27.md)) |
| `requirements-viz.txt` | Optional deps for the Dash viewer (`dash`, `plotly`, `dash-cytoscape`, `pandas`) |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | PR/push CI: Python only, `pytest -m "not spark"` (fast; Polars + non-Spark tests) |
| [`.github/workflows/ci-spark.yml`](.github/workflows/ci-spark.yml) | Optional Spark suite: `pytest -m spark` (Temurin 17); `workflow_dispatch` + weekly schedule |

## Requirements

- Python 3.10+
- PySpark 3.5.x (Spark 3.5 compatible)
- Java for full Spark tests, local Spark generation, and BigQuery via the connector. The default PR CI job does not require Java; it runs tests marked `not spark` only.
- For production-style runs: a Spark session that can write BigQuery (Spark connector + GCS staging bucket), or Delta/Parquet to a path or metastore-backed table your cluster supports. Spark-backed tests use `local[2]` when Java is available.

## License

No license file is included in this repository; add one if you intend to redistribute or contribute under explicit terms.
