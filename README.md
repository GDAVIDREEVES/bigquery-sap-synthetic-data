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

## Streamlit

```bash
streamlit run acdoca_generator/app.py
```

Point the app at a Spark session with access to your catalog for Delta writes; see `acdoca_generator/utils/spark_writer.py` for write paths and options.

## License

No license file is included in this repository; add one if you intend to redistribute or contribute under explicit terms.
