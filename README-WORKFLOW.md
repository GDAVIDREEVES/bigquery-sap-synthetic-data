# ACDOCA Generator — Workflow Guide

Step-by-step recipes for the most common tasks. For feature reference and architecture, see [README.md](README.md). For deep design intent, see [SPEC-ACDOCA-Synthetic-Generator.md](SPEC-ACDOCA-Synthetic-Generator.md).

The **primary** way to run this solution is the BigQuery Studio Jupyter notebook ([Workflow A](#a-bigquery-studio-jupyter-notebook-primary)). The CLI ([Workflow B](#b-cli--bigquery-laptop-or-dataproc)) is the high-volume / scripted alternative. Other workflows are for development and inspection.

## Pick a workflow

| If you want to… | Go to |
|---|---|
| Set up the repo for the first time | [0. First-time setup](#0-first-time-setup) |
| Run the **standard, supported** path: interactive form inside the BigQuery console | [A. BigQuery Studio Jupyter notebook (primary)](#a-bigquery-studio-jupyter-notebook-primary) |
| Push high-volume / scripted runs into BigQuery from your laptop or Dataproc | [B. CLI → BigQuery (laptop or Dataproc)](#b-cli--bigquery-laptop-or-dataproc) |
| Run a parameterized PySpark notebook on Dataproc / Vertex Workbench | [C. Dataproc / Vertex notebook](#c-dataproc--vertex-notebook) |
| Build a DataFrame in Python for tests or downstream code | [D. Programmatic API (local Spark / Polars)](#d-programmatic-api-local-spark--polars) |
| Visualize a supply-chain run as a network graph | [E. Dash supply-chain viewer](#e-dash-supply-chain-viewer) |
| Run the test suite | [F. Tests](#f-tests) |
| Fix a broken local Spark or BQ run | [Troubleshooting](#troubleshooting) |

---

## 0. First-time setup

Once per machine. Skip this if you'll only use Workflow A — the BQ Studio runtime installs the package itself.

1. **Clone the repo and enter it.**
   ```bash
   git clone https://github.com/GDAVIDREEVES/bigquery-sap-synthetic-data.git
   cd bigquery-sap-synthetic-data
   ```
2. **Create a virtual environment.** Spark 3.5 is most stable on Python 3.11.
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate            # Windows: .venv\Scripts\activate
   ```
3. **Install the package and the test/Polars extras.**
   ```bash
   pip install -r requirements.txt
   pip install -e ".[dev,fast]"
   ```
   Add `.[viz]` if you'll use the Dash supply-chain viewer ([Workflow E](#e-dash-supply-chain-viewer)).
4. **Install Java only if you'll run Spark locally** (workflows B, D, F-spark — anything that hits BigQuery from your laptop).
   ```bash
   # macOS
   brew install --cask temurin@17
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   ```
5. **Smoke-test the install** with the fast suite (no Java required):
   ```bash
   pytest -m "not spark"
   ```

---

## A. BigQuery Studio Jupyter notebook (primary)

This is the supported path and how recent successful runs have been done. The notebook is [`notebooks/02_generate_acdoca_bq_studio.ipynb`](notebooks/02_generate_acdoca_bq_studio.ipynb). It clones the repo into the BQ Studio runtime, installs the package, and renders an `ipywidgets` form (preset, industry, country multi-select, complexity, sliders, opt-in checkboxes for supply chain / segment P&L / year-end trueup, Generate button). Clicking **Generate** writes to BigQuery using the same code path as the CLI.

You authenticate as your Google identity (no service-account JSON), and the runtime ships with Spark + the BigQuery connector pre-wired — no `spark.jars.packages` and no GCS Hadoop connector JAR to manage.

**Prereqs:** GCP project with BigQuery + GCS enabled; you can sign in to the BigQuery console.

### A.1 — One-time GCP setup

1. **Create the BigQuery dataset.** Open [`notebooks/00_bq_setup.sql`](notebooks/00_bq_setup.sql), replace `MY_PROJECT` with your project ID, and run it (BQ console **Compose new query** → paste → **Run**, or `bq query --use_legacy_sql=false < notebooks/00_bq_setup.sql`).
2. **Create a GCS staging bucket** in the same region as your dataset and grant your **user** identity `roles/storage.objectAdmin` on it:
   ```bash
   gsutil mb -p $PROJECT_ID -l US gs://your-staging-bucket
   gcloud storage buckets add-iam-policy-binding gs://your-staging-bucket \
     --member="user:you@example.com" --role="roles/storage.objectAdmin"
   ```
3. **Confirm BigQuery permissions.** Your user needs `roles/bigquery.dataEditor` on the target dataset (or project). Most owners/editors already have this.

### A.2 — Open the notebook in BigQuery Studio

1. In the BigQuery console: **+ Add → Python notebook**, then choose the **PySpark** runtime when prompted.
2. **Import** [`notebooks/02_generate_acdoca_bq_studio.ipynb`](notebooks/02_generate_acdoca_bq_studio.ipynb): use the file uploader, or open the file locally, copy its contents, and paste into a new notebook.
3. (Alternative for repeat use) host the `.ipynb` in a Cloud Storage bucket and open it via **File → Open from URL**.

### A.3 — Configure the run

1. **Run cell 1 (Setup).** It clones the repo into `/home/jupyter/repo` and `pip install -e`s the package into the kernel. On a fresh kernel this takes ~30–60 s.
2. **Edit cell 2 (Configure).** Set:
   ```python
   PROJECT_ID = "your-gcp-project"
   BQ_DATASET = "synthetic_acdoca"      # created in A.1 step 1
   BQ_TABLE   = "journal_entries"        # connector creates on first write
   GCS_BUCKET = "your-staging-bucket"    # created in A.1 step 2
   ```
3. **Run cell 3 (Imports + Spark session).** `getOrCreate()` reuses BQ Studio's pre-wired Spark.
4. **Run cell 4 (Form).** An `ipywidgets` panel appears with: preset, industry, country multi-select, complexity, sliders for txn-per-CC-per-period / IC% / SC chain count / challenged-share, and checkboxes for supply-chain, segment P&L, and year-end trueup.

### A.4 — Generate

1. **Pick a preset** to seed the form. Start with `quick_smoke` (~5K rows, ~6 s) to confirm wiring before scaling up. Six presets ship: `quick_smoke`, `globe_lite`, `tp_workshop`, `supply_chain_demo`, `ml_features`, `controversy_demo`. Pick `custom` to tune everything manually.
2. **(Optional) tick realism extras** — supply chain, segment P&L, year-end trueup — to populate the supplementary DataFrames alongside the 538-column ACDOCA frame.
3. **Click Generate.** The cell prints row counts, validation results, and the resolved table name. The connector creates the table on first write with `BUDAT` MONTH partitioning and clustering on `RBUKRS / GJAHR / POPER`.

### A.5 — Verify the write

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) AS n, MIN(BUDAT) AS min_budat, MAX(BUDAT) AS max_budat
   FROM \`PROJECT.DATASET.TABLE\`"
```

Confirm partition/cluster layout:

```bash
bq show --format=prettyjson PROJECT:DATASET.TABLE | jq '{timePartitioning, clustering}'
```

Generator metadata is attached as **table labels** (`bigQueryTableLabel.*`) — visible in the BQ console under **Details → Labels**.

### A.6 — Iterate

To scale up after `quick_smoke` works: change the **Preset** dropdown to `tp_workshop`, `supply_chain_demo`, or `ml_features`, then re-click **Generate**. Each click produces another partition-aligned write to the same table. To start clean, drop and recreate the table:

```bash
bq rm -f -t PROJECT:DATASET.TABLE
```

---

## B. CLI → BigQuery (laptop or Dataproc)

Best for high-volume runs (`ml_features`, ~1M-row custom configs), CI canaries, and scripted jobs. No form, but no JVM-heap ceiling beyond your machine's RAM.

**Prereqs:** [Workflow 0](#0-first-time-setup) done; Java installed; A.1 (GCP setup) done.

### B.1 — One-time local credential setup

Either:
- ADC: `gcloud auth application-default login`, **or**
- Service-account key: `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json`

The Spark driver identity needs `roles/bigquery.dataEditor` on the dataset and `roles/storage.objectAdmin` on the staging bucket.

### B.2 — One-time GCS Hadoop connector install

The Spark BQ connector stages temp files via `gs://`, which needs the GCS Hadoop FileSystem on the classpath. **Without this JAR, BQ writes fail with `UnsupportedFileSystemException: No FileSystem for scheme "gs"`.**

```bash
mkdir -p ~/.spark-jars
curl -L -o ~/.spark-jars/gcs-connector-hadoop3-2.2.21-shaded.jar \
  https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.21/gcs-connector-hadoop3-2.2.21-shaded.jar
```

The CLI auto-detects this path (override with `ACDOCA_SPARK_GCS_JAR`). On Dataproc it's bundled — skip this step.

### B.3 — Run the CLI

1. **Set the staging bucket** (or pass `--gcs-temp-bucket`):
   ```bash
   export ACDOCA_GCS_TEMP_BUCKET=your-staging-bucket
   ```
2. **Smoke-run with `quick_smoke`** to verify credentials, IAM, and connectors:
   ```bash
   python scripts/run_generate_bq.py \
     --preset quick_smoke \
     --full-table-name YOUR_PROJECT.synthetic_acdoca.journal_entries
   ```
3. **Verify** with the same `bq query` shown in [A.5](#a5--verify-the-write). Expect ~4,800 rows for `quick_smoke`.
4. **Scale up.** Larger presets need more driver memory:
   ```bash
   SPARK_DRIVER_MEMORY=8g python scripts/run_generate_bq.py \
     --preset ml_features \
     --full-table-name YOUR_PROJECT.synthetic_acdoca.journal_entries
   ```
5. **Custom configs** (~1M rows for a 10-country pharma scenario):
   ```bash
   SPARK_DRIVER_MEMORY=8g python scripts/run_generate_bq.py \
     --preset custom \
     --industry-key pharmaceutical \
     --country-isos US,DE,CH,IE,IN,FR,GB,JP,IT,ES \
     --complexity very_high \
     --txn-per-cc-per-period 3500 \
     --ic-pct 0.35 \
     --sc-chains 100 \
     --full-table-name YOUR_PROJECT.synthetic_acdoca.journal_entries
   ```
6. **All flags:** `python scripts/run_generate_bq.py --help`. Override the connector package with `ACDOCA_SPARK_BQ_PACKAGE` for non-default Spark versions.

### B.4 — Submit on Dataproc

1. Install this package on the cluster (initialization action or custom image with `pip install -e .`).
2. Attach the BigQuery connector — either a Dataproc image that includes `spark-3.5-bigquery`, or `--packages com.google.cloud.spark:spark-3.5-bigquery:0.44.1` on `spark-submit`.
3. `spark-submit scripts/run_generate_bq.py -- --preset ... --full-table-name ...` (note the `--` separator before script args).

For runtime-selection guidance (BQ Studio default vs Vertex AI Workbench vs laptop CLI), see [`docs/runtime-options.md`](docs/runtime-options.md).

---

## C. Dataproc / Vertex notebook

Use [`notebooks/01_generate_acdoca_bq.py`](notebooks/01_generate_acdoca_bq.py) — a parameterized PySpark notebook (cells delimited by `# %%`). Same code path as the CLI; designed for Dataproc Workbench or Vertex AI Workbench managed notebooks where you want to override parameters via environment variables (`ACDOCA_PRESET`, `ACDOCA_INDUSTRY_KEY`, etc.).

1. Open the file in your notebook environment (Workbench's editor handles `# %%` cells natively).
2. Ensure the cluster or session has the Spark BigQuery connector JAR (Dataproc image with `spark-3.5-bigquery`, or set `spark.jars.packages` as in [`scripts/run_generate_bq.py`](scripts/run_generate_bq.py)).
3. Set `ACDOCA_FULL_TABLE_NAME` and `ACDOCA_GCS_TEMP_BUCKET` (or accept the defaults at the top of the file).
4. Run cells top-to-bottom.

---

## D. Programmatic API (local Spark / Polars)

For integration tests and building on top of the generator. Not the path for ad-hoc data generation — use Workflow A or B for that.

**Prereqs:** [Workflow 0](#0-first-time-setup) done; Java installed.

1. **Set Spark env vars:**
   ```bash
   export SPARK_DRIVER_MEMORY=8g
   export PYSPARK_PYTHON="$(which python)"
   export SPARK_LOCAL_IP=127.0.0.1
   ```
2. **Run the bundled smoke script** to confirm everything wires up:
   ```bash
   python scripts/dev_smoke_supply_chain.py
   ```
   It generates a small pharma dataset with supply chain + segment P&L, asserts IC docs balance to zero, prints diagnostics, and writes `flows.json` for Workflow E.
3. **Adapt for your own run:**
   ```python
   from pyspark.sql import SparkSession
   from acdoca_generator.generators.pipeline import (
       GenerationConfig, generate_acdoca_dataframe,
   )

   spark = (
       SparkSession.builder.appName("acdoca")
       .master("local[2]")
       .config("spark.sql.shuffle.partitions", "4")
       .getOrCreate()
   )
   cfg = GenerationConfig(
       industry_key="pharmaceutical",
       country_isos=["US", "DE", "IE", "CH"],
       fiscal_year=2025,
       fiscal_variant="calendar",
       complexity="medium",
       txn_per_cc_per_period=2,
       seed=42,
       include_supply_chain=True,
       sc_chains_per_period=12,
       include_segment_pl=True,
   )
   result = generate_acdoca_dataframe(spark, cfg)
   result.acdoca_df.show(5)
   result.supply_chain_flows_df.show(5)
   result.segment_pl_df.show(5)
   ```
4. **(Optional) export flows** for the Dash viewer:
   ```python
   from acdoca_generator.generators.pipeline import export_supply_chain_json
   export_supply_chain_json(result.supply_chain_flows_df, "flows.json")
   ```

**Polars-only fast path** (no JVM, domestic-only — no IC, no supply chain): import from [`acdoca_generator.core`](acdoca_generator/core/) (`build_companies_indexed_polars`, `domestic_balanced_polars`). Use `acdoca_generator.spark_bridge.polars_to_spark` if you later need to feed a Spark writer.

---

## E. Dash supply-chain viewer

Renders a `supply_chain_flows_df` as an interactive network graph (Cytoscape).

**Prereqs:** [Workflow 0](#0-first-time-setup) done; viz extras installed (`pip install -e ".[viz]"`); a `flows.json` to load.

1. **Produce a `flows.json`** — easiest path: run [D.2](#d-programmatic-api-local-spark--polars) (`python scripts/dev_smoke_supply_chain.py` writes `flows.json` to the repo root). Alternatively, call `export_supply_chain_json` after a programmatic run.
2. **Launch the viewer:**
   ```bash
   python -m acdoca_generator.dash_app.app --data flows.json
   ```
   Defaults: `--host 127.0.0.1 --port 8050`. Add `--debug` for hot reload. If 8050 is taken: `--port 8051`.
3. **Open** [http://127.0.0.1:8050](http://127.0.0.1:8050) and keep the terminal open while you browse. Stop with Ctrl-C.

Works with Dash 2 and Dash 3 (see `requirements-viz.txt`).

---

## F. Tests

### F.1 — Default fast suite (recommended for routine checks)

Seconds, no Java:

```bash
pytest -m "not spark"
```

This is what CI runs on every PR ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)).

### F.2 — Spark integration suite

Many minutes; needs JDK 17+ and PySpark:

```bash
export SPARK_LOCAL_IP=127.0.0.1
pytest -m spark -vv
```

### F.3 — Verifying the harness (reliability gates)

Two opt-in suites guard the larger presets:

1. **All presets, full pipeline, no BQ write** (~1 min on a laptop):
   ```bash
   ACDOCA_RUN_SPARK_TESTS=1 SPARK_DRIVER_MEMORY=4g \
     pytest -m spark acdoca_generator/tests/test_presets_full_pipeline.py
   ```
2. **Real BigQuery write canary** (writes ~30K rows, verifies via `bq query`, drops the table):
   ```bash
   ACDOCA_RUN_SPARK_TESTS=1 ACDOCA_RUN_BQ_TESTS=1 \
     ACDOCA_BQ_TABLE=your-project.your_dataset.canary \
     ACDOCA_GCS_TEMP_BUCKET=your-staging-bucket \
     pytest acdoca_generator/tests/test_bq_write_canary.py
   ```

The supply-chain Spark test is also opt-in — it only runs when `ACDOCA_RUN_SPARK_TESTS=1` is set.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| BQ Studio: `ModuleNotFoundError: acdoca_generator` | Setup cell didn't finish or kernel restarted | Re-run cell 1 (Setup). If git pull failed, delete `/home/jupyter/repo` and run again. |
| BQ Studio: `Permission denied` writing to GCS | Your user lacks `objectAdmin` on the staging bucket | Re-check [A.1 step 2](#a1--one-time-gcp-setup). |
| BQ Studio: `403` on dataset access | Missing `bigquery.dataEditor` | Re-check [A.1 step 3](#a1--one-time-gcp-setup). |
| Local: `Java gateway process exited` / `JAVA_HOME is not set` | No JDK on PATH | Install Temurin 17, set `JAVA_HOME` (see [step 0.4](#0-first-time-setup)). |
| Local: `OutOfMemoryError` in Spark, even on small runs | Default 1g driver heap | `export SPARK_DRIVER_MEMORY=4g` (or 8g for `ml_features`). |
| Local: `PYTHON_VERSION_MISMATCH` between driver and workers | Workers found system Python (often 3.9) | `export PYSPARK_PYTHON="$(which python)"` before launching. |
| Local: Spark hangs on `bind` / hostname errors | Hostname resolution flaky on laptops/CI | `export SPARK_LOCAL_IP=127.0.0.1`. |
| Local CLI: `UnsupportedFileSystemException: No FileSystem for scheme "gs"` | GCS Hadoop connector JAR missing | Run [B.2](#b2--one-time-gcs-hadoop-connector-install). |
| Local CLI: BQ write fails with `403` / permission denied | Spark identity lacks BQ or GCS roles | Re-check [B.1](#b1--one-time-local-credential-setup). |
| `pytest -m "not spark"` fails on Polars import | `[fast]` extras not installed | `pip install -e ".[dev,fast]"`. |
| Obscure JVM errors only on Python 3.12 | Spark 3.5 / Py3.12 edge case | Try a Python 3.11 venv. The supply-chain JSON exporter Py3.12 issue itself is fixed (commit `10d642e`). |
| Dash viewer: port already in use | 8050 taken | `python -m acdoca_generator.dash_app.app --data flows.json --port 8051`. |
| Spark run is slow on small data | Expected — JVM startup dominates on tiny volumes | Use the Polars `core` path for fast iteration ([Workflow D](#d-programmatic-api-local-spark--polars), last paragraph). |

For runtime-selection guidance (BQ Studio vs Vertex Workbench vs laptop CLI), see [`docs/runtime-options.md`](docs/runtime-options.md). For realism findings and known scenario gaps, see [`diagnostics/`](diagnostics/).
