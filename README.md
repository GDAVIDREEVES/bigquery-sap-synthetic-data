# ACDOCA Synthetic Data Generator

Synthetic SAP S/4HANA **ACDOCA** (Universal Journal) data for **Databricks** (Unity Catalog, Delta Lake) and local Spark. Use it for transfer-pricing analysis, intercompany testing, financial supply-chain modeling, Pillar Two / GloBE demos, and ML pipelines without production SAP extracts.

Design and column coverage follow [SPEC-ACDOCA-Synthetic-Generator.md](SPEC-ACDOCA-Synthetic-Generator.md).

## Features

- **538-column schema** aligned with the spec appendix (sanitized names, e.g. `.INCLU-_PN` → `INCLU_PN`).
- **Industry templates** drive domestic **GL mix**, **posting period seasonality**, default **intercompany share**, and optional **entity-role** hints per country.
- **Streamlit UI** (`acdoca_generator/app.py`) with **named demo presets**, custom parameters, and **strict vs fast** validation.
- **Spark generators**: master data, domestic transactions, intercompany pairs, amounts/currency, closing patterns, document numbering.
- **Validators**: debit/credit balance and IC reconciliation checks (full PK uniqueness scan optional).
- **Delta writer** with schema enforcement for catalog-backed tables; `generator.version` is filled from the installed **package version** when not set explicitly.

## Project layout

| Path | Role |
|------|------|
| `acdoca_generator/app.py` | Streamlit entry point |
| `acdoca_generator/config/` | Industries, presets, countries, chart of accounts, field tiers, operating models |
| `acdoca_generator/generators/` | Pipeline, master data, transactions, intercompany, amounts, closing, document |
| `acdoca_generator/validators/` | Balance and consistency checks |
| `acdoca_generator/utils/` | Spark schema and Delta writer |
| `notebooks/` | Databricks SQL + Python notebooks (UC setup, parameterized generation) |
| `databricks.yml` | Optional Databricks Asset Bundle (sample job) |
| `.github/workflows/ci.yml` | GitHub Actions: JDK 17 + pytest |
| `scripts/complete_github_ssh.sh` | After registering your SSH key on GitHub, run to verify `ssh` and push `main` |

## Requirements

- Python **3.10+**
- **PySpark** 3.5.x (Spark 3.5 compatible)
- **Java** (for local Spark tests and generation)
- For production-style runs: a Spark session that can write to your **Unity Catalog** Delta tables (e.g. Databricks cluster / DBR). Local `pytest` uses Spark in `local[2]` mode when Java is available.

## Install

```bash
git clone https://github.com/GDAVIDREEVES/databricks-sap-synthetic-data.git
cd databricks-sap-synthetic-data
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

Install **test** dependencies (pytest is not in `requirements.txt`):

```bash
pip install -e ".[dev]"
```

## Tests

```bash
pip install -e ".[dev]"
pytest
```

**Java:** PySpark needs a JDK on the machine that runs tests. Without it, Spark-backed tests are skipped. Install **Temurin 17** (or another JDK 11+) and ensure `java -version` works; on macOS, `brew install --cask temurin@17` and set `JAVA_HOME` if needed (`/usr/libexec/java_home -v 17`).

**Faster local runs:** The session-scoped Spark fixture can make a full `pytest` take several minutes. For a quick check: `pytest acdoca_generator/tests/test_industry_alias.py -v`.

CI: [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs on push/PR to `main` with Temurin JDK 17 and Python 3.11 (`pip install -e ".[dev]"` then `pytest`).

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

## Streamlit

```bash
streamlit run acdoca_generator/app.py
```

Point the app at a Spark session with access to your catalog for Delta writes; see `acdoca_generator/utils/spark_writer.py` for write paths and options.

## Git and GitHub (SSH)

This repo uses an SSH remote (`git@github.com:...`). On macOS, generate a key (`ssh-keygen -t ed25519`), add `~/.ssh/id_ed25519.pub` under **GitHub → Settings → SSH and GPG keys**, then confirm with `ssh -T git@github.com`.

Cursor uses your system Git and `~/.ssh` (same as Terminal). After SSH works, you can push from Cursor or run:

```bash
./scripts/complete_github_ssh.sh
```

## License

No license file is included in this repository; add one if you intend to redistribute or contribute under explicit terms.
