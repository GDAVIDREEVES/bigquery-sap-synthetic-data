"""Streamlit entry point for Databricks Apps (SPEC §3)."""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root on path when running `streamlit run acdoca_generator/app.py`
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from acdoca_generator.config.countries import COUNTRIES
from acdoca_generator.config.field_tiers import COMPLEXITY_LEVELS, fields_for_complexity
from acdoca_generator.config.industries import industry_keys
from acdoca_generator.generators.pipeline import GenerationConfig, generate_acdoca_dataframe
from acdoca_generator.utils.schema import acdoca_schema
from acdoca_generator.utils.spark_writer import GenerationParams, write_acdoca_table
from acdoca_generator.validators.balance import blocking_failures, run_validations


def _spark() -> SparkSession:
    return SparkSession.builder.appName("ACDOCA_Synthetic_Generator").getOrCreate()


def _country_options():
    return [(c.display_name, c.iso) for c in COUNTRIES]


def main() -> None:
    st.set_page_config(page_title="ACDOCA Synthetic Generator", layout="wide")
    st.title("ACDOCA Synthetic Data Generator")

    st.subheader("Industry template")
    ind_labels = {k: k.replace("_", " ").title() for k in industry_keys()}
    industry = st.radio(
        "Industry",
        options=list(ind_labels.keys()),
        format_func=lambda k: ind_labels[k],
        horizontal=True,
    )

    st.subheader("Countries")
    opts = _country_options()
    all_isos = [o[1] for o in opts]
    name_by_iso = {iso: name for name, iso in opts}
    selected_names = st.multiselect(
        "Countries (ISO stored as GB for United Kingdom)",
        options=[name_by_iso[i] for i in all_isos],
        default=[name_by_iso[i] for i in ("US", "DE", "GB") if i in name_by_iso],
    )
    selected_isos = [next(iso for name, iso in opts if name == n) for n in selected_names]
    if not selected_isos:
        st.warning("Select at least one country.")
        selected_isos = ["US"]

    st.subheader("Fiscal year")
    fy_col1, fy_col2 = st.columns(2)
    with fy_col1:
        fiscal_year = st.selectbox("Year", options=list(range(2020, 2031)), index=6)
    with fy_col2:
        fiscal_variant = st.radio(
            "Fiscal calendar",
            options=["calendar", "april"],
            format_func=lambda x: "Calendar (Jan–Dec)" if x == "calendar" else "April (Apr–Mar)",
            horizontal=True,
        )

    st.subheader("Complexity tier")
    complexity = st.radio(
        "Tier",
        options=list(COMPLEXITY_LEVELS),
        format_func=lambda c: {
            "light": "Light (~55 fields)",
            "medium": "Medium (~130 fields)",
            "high": "High (~250 fields)",
            "very_high": "Very High (~400+ fields)",
        }[c],
        horizontal=True,
    )

    st.subheader("Volume")
    txn_per = st.slider("Transactions per company code per period", 100, 50_000, 1_000, step=100)
    ic_pct = st.slider("Intercompany % of lines", 5, 60, 25)
    rev = st.checkbox("Include reversals (~5% flagged)", value=True)
    closing = st.checkbox("Include closing entries", value=True)
    seed = st.number_input("Random seed", min_value=0, value=42, step=1)

    st.subheader("Output")
    target = st.text_input("Target catalog.schema.table", value="synthetic.acdoca.journal_entries")
    fmt = st.radio("Format", options=["delta", "parquet"], horizontal=True)
    parquet_path = None
    if fmt == "parquet":
        parquet_path = st.text_input("Parquet path (DBFS or UC volume)", value="/tmp/acdoca_synthetic")

    if st.button("Generate", type="primary"):
        spark = _spark()
        cfg = GenerationConfig(
            industry_key=industry,
            country_isos=selected_isos,
            fiscal_year=int(fiscal_year),
            fiscal_variant=str(fiscal_variant),
            complexity=str(complexity),
            txn_per_cc_per_period=int(txn_per),
            ic_pct=float(ic_pct) / 100.0,
            include_reversals=bool(rev),
            include_closing=bool(closing),
            seed=int(seed),
        )
        bar = st.progress(0.0, text="Generating…")
        try:
            df = generate_acdoca_dataframe(spark, cfg)
            bar.progress(0.5, text="Validating…")
            results = run_validations(df)
            fails = blocking_failures(results)
            bar.progress(0.75, text="Writing…")
            if not fails:
                write_acdoca_table(
                    spark,
                    df,
                    full_table_name=target,
                    gen=GenerationParams(
                        industry=industry,
                        complexity=complexity,
                        countries_iso_csv=",".join(selected_isos),
                        fiscal_year=int(fiscal_year),
                        seed=int(seed),
                    ),
                    output_format=fmt,
                    parquet_path=parquet_path,
                )
            bar.progress(1.0, text="Done")
        except Exception as e:
            st.exception(e)
            return

        st.subheader("Validation")
        for r in results:
            st.write(f"**{r.name}** — {r.display_severity}: {r.detail}")

        if fails:
            st.error("Write skipped: blocking validation failures (FAIL).")
        else:
            st.success(f"Written to **{target}**" if fmt == "delta" else f"Parquet written to **{parquet_path}**")

        st.subheader("Summary")
        total = df.count()
        st.metric("Total rows", f"{total:,}")
        by_cc = df.groupBy("RBUKRS").count().orderBy("RBUKRS").collect()
        st.write("Row count by company code")
        st.dataframe([r.asDict() for r in by_cc], use_container_width=True)

        deb = df.filter(df.DRCRK == "S").select(F.sum("WSL")).collect()[0][0]
        cred = df.filter(df.DRCRK == "H").select(F.sum("WSL")).collect()[0][0]
        deb = float(deb or 0)
        cred = float(cred or 0)
        st.write(f"Sum WSL debits (S): {deb} / credits (H): {cred} (should net to ~0)")

        ic_n = df.filter(df.RASSC != "").count()
        ic_ok = next((r.passed for r in results if r.name == "IC_PAIR"), True)
        ic_amt_ok = next((r.passed for r in results if r.name == "IC_AMOUNT"), True)
        st.write(f"IC line count: {ic_n}; IC_PAIR: {'pass' if ic_ok else 'warn'}; IC_AMOUNT: {'pass' if ic_amt_ok else 'warn'}")

        populated = fields_for_complexity(complexity)
        n_cols = len(acdoca_schema().fields)
        st.write(f"Field coverage: **{len(populated)}** of **{n_cols}** columns may be populated at this tier")

        st.subheader("Sample (100 rows)")
        st.dataframe(df.limit(100).toPandas(), use_container_width=True)


if __name__ == "__main__":
    main()
