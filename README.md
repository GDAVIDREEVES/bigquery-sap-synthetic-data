# ACDOCA Synthetic Data Generator

Synthetic SAP S/4HANA **ACDOCA** (Universal Journal) data for **Databricks** (Unity Catalog, Delta Lake) and local Spark. Use it for transfer-pricing analysis, intercompany testing, financial supply-chain modeling, Pillar Two / GloBE demos, and ML pipelines without production SAP extracts.

Design and column coverage follow [SPEC-ACDOCA-Synthetic-Generator.md](SPEC-ACDOCA-Synthetic-Generator.md).

## Features

- **538-column schema** aligned with the spec appendix (sanitized names, e.g. `.INCLU-_PN` → `INCLU_PN`).
- **Streamlit UI** (`acdoca_generator/app.py`) for industry template, countries, fiscal year, and complexity tier.
- **Spark generators**: master data, domestic transactions, intercompany pairs, amounts/currency, closing patterns, document numbering.
- **Validators**: debit/credit balance and IC reconciliation checks.
- **Delta writer** with schema enforcement for catalog-backed tables.

## Project layout

| Path | Role |
|------|------|
| `acdoca_generator/app.py` | Streamlit entry point |
| `acdoca_generator/config/` | Industries, countries, chart of accounts, field tiers, operating models |
| `acdoca_generator/generators/` | Pipeline, master data, transactions, intercompany, amounts, closing, document |
| `acdoca_generator/validators/` | Balance and consistency checks |
| `acdoca_generator/utils/` | Spark schema and Delta writer |
| `notebooks/` | Databricks SQL + Python notebooks (UC setup, parameterized generation) |
| `databricks.yml` | Optional Databricks Asset Bundle (sample job) |
| `scripts/complete_github_ssh.sh` | After registering your SSH key on GitHub, run to verify `ssh` and push `main` |

## Requirements

- Python **3.10+**
- **PySpark** 3.5.x (Spark 3.5 compatible)
- For production-style runs: a Spark session that can write to your **Unity Catalog** Delta tables (e.g. Databricks cluster / DBR). Local `pytest` uses Spark in `local[2]` mode.

## Install

```bash
git clone https://github.com/GDAVIDREEVES/databricks-sap-synthetic-data.git
cd databricks-sap-synthetic-data
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
```

Optional dev install (pytest already in `requirements.txt`):

```bash
pip install -e ".[dev]"
```

## Tests

```bash
pytest
```

## Databricks (recommended)

This repo includes Databricks-ready notebooks and an optional Databricks Asset Bundle job definition.

### 1) Create the Unity Catalog schema

Run:

- `notebooks/00_uc_setup.sql`

It creates `synthetic` + `synthetic.acdoca` (adjust to your naming / permissions model).

### 2) Run the generator notebook

Run:

- `notebooks/01_generate_acdoca.py`

It takes parameters via `dbutils.widgets` (or Job “base parameters”):

- `industry_key` (default `consumer_products`)
- `country_isos_csv` (default `US,DE,GB`)
- `fiscal_year` (default `2026`)
- `fiscal_variant` (`calendar` or `april`)
- `complexity` (`light|medium|high|very_high`)
- `txn_per_cc_per_period` (default `1000`)
- `ic_pct` (default `0.25` as a fraction \(0..1\))
- `include_reversals` / `include_closing` (`true|false`)
- `seed` (default `42`)
- `full_table_name` (default `synthetic.acdoca.journal_entries`)
- `output_format` (`delta|parquet`) and optional `parquet_path`

### 3) Optional: deploy as a job with a Databricks Asset Bundle

This repo includes `databricks.yml` with a sample job. To use it:

- Set `DATABRICKS_HOST` and `DATABRICKS_NODE_TYPE_ID`
- Use the Databricks CLI bundle workflow (e.g. `databricks bundle deploy`, `databricks bundle run`)

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
