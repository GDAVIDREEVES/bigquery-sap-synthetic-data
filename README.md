# ACDOCA Synthetic Data Generator

Databricks-oriented generator for SAP S/4HANA **ACDOCA**-shaped synthetic journals, per [SPEC-ACDOCA-Synthetic-Generator.md](SPEC-ACDOCA-Synthetic-Generator.md).

## Layout

Python package `acdoca_generator/` with Streamlit UI (`acdoca_generator/app.py`), Spark generators, validators, and a 538-column schema aligned to the spec appendix (sanitized names: `.INCLU-_PN` → `INCLU_PN`, etc.).

## Local setup

```bash
cd /path/to/databricks-sap-synthetic-data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Tests

```bash
pytest
```

## Streamlit

```bash
streamlit run acdoca_generator/app.py
```

Use a Spark session that can reach your catalog for Delta writes (e.g. Databricks cluster / DBR). For local tests, Spark runs in `local[2]` mode.
