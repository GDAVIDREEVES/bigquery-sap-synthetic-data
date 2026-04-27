"""Streamlit entry point for local PySpark generation (SPEC §3)."""

from __future__ import annotations

import os
import sys
import tempfile
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
from acdoca_generator.config.industries import canonical_industry_key, industry_keys
from acdoca_generator.config.presets import DEMO_PRESETS, preset_keys
from acdoca_generator.generators.pipeline import (
    GenerationConfig,
    export_supply_chain_json,
    generate_acdoca_dataframe,
)
from acdoca_generator.utils.schema import acdoca_schema
from acdoca_generator.utils.spark_writer import GenerationParams, write_acdoca_table
from acdoca_generator.validators.balance import blocking_failures, run_validations

_SPARK_BQ_PACKAGE = os.environ.get(
    "ACDOCA_SPARK_BQ_PACKAGE",
    "com.google.cloud.spark:spark-3.5-bigquery:0.44.1",
)


def _spark(*, for_bigquery: bool = False) -> SparkSession:
    if for_bigquery:
        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
        return (
            SparkSession.builder.appName("ACDOCA_Synthetic_Generator_BQ")
            .config("spark.jars.packages", _SPARK_BQ_PACKAGE)
            .getOrCreate()
        )
    return SparkSession.builder.appName("ACDOCA_Synthetic_Generator").getOrCreate()


def _country_options():
    return [(c.display_name, c.iso) for c in COUNTRIES]


def main() -> None:
    st.set_page_config(page_title="ACDOCA Synthetic Generator", layout="wide")
    st.title("ACDOCA Synthetic Data Generator")

    st.subheader("Demo preset")
    preset_choice_options = ["custom"] + preset_keys()
    preset_labels = {"custom": "Custom (use widgets below)"}
    preset_labels.update({k: DEMO_PRESETS[k].label for k in preset_keys()})
    preset_choice = st.selectbox(
        "Preset",
        options=preset_choice_options,
        format_func=lambda k: preset_labels[k],
        help="Presets tune volume, IC share, and validation for faster repeat demos.",
    )

    industry = None
    selected_isos = None
    fiscal_year = None
    fiscal_variant = None
    complexity = None
    txn_per = None
    ic_pct = None
    use_industry_ic = False
    rev = None
    closing = None
    validation_profile = "strict"
    include_supply_chain = False
    sc_chains_per_period = 50

    if preset_choice == "custom":
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
        use_industry_ic = st.checkbox(
            "Use industry default IC % (template ic_share_default)",
            value=False,
        )
        ic_pct = st.slider("Intercompany % of lines", 5, 60, 25, disabled=use_industry_ic)
        rev = st.checkbox("Include reversals (~5% flagged)", value=True)
        closing = st.checkbox("Include closing entries", value=True)
        include_supply_chain = st.checkbox(
            "Include financial supply chain (multi-hop IC + flow table)",
            value=False,
            help="Adds synthetic supply-chain hops and paired IC lines. Export JSON for the Dash viewer.",
        )
        sc_chains_per_period = st.slider(
            "Supply chain flows to generate",
            min_value=1,
            max_value=500,
            value=50,
            disabled=not include_supply_chain,
        )
        include_segment_pl = st.checkbox(
            "Compute segment P&L (entity × role × segment × period rollup)",
            value=False,
            help="Aggregates ACDOCA into operating-margin views suitable for TP analytics.",
        )

        st.subheader("Validation")
        validation_profile = st.radio(
            "Validation profile",
            options=["strict", "fast"],
            format_func=lambda x: "Strict (full PK uniqueness scan)" if x == "strict" else "Fast (skip PK full scan)",
            horizontal=True,
        )
    else:
        pr = DEMO_PRESETS[preset_choice]
        st.info(
            f"**{pr.label}** — industry `{pr.industry_key}`, countries `{pr.country_isos_csv}`, "
            f"FY **{pr.fiscal_year}** ({pr.fiscal_variant}), complexity **{pr.complexity}**, "
            f"**{pr.txn_per_cc_per_period}** txn/cc/period, validation **{pr.validation_profile}**."
        )
        industry = pr.industry_key
        selected_isos = [x.strip() for x in pr.country_isos_csv.split(",") if x.strip()]
        fiscal_year = pr.fiscal_year
        fiscal_variant = pr.fiscal_variant
        complexity = pr.complexity
        txn_per = pr.txn_per_cc_per_period
        ic_pct = pr.ic_pct
        rev = pr.include_reversals
        closing = pr.include_closing
        validation_profile = pr.validation_profile
        include_supply_chain = bool(pr.include_supply_chain)
        sc_chains_per_period = int(pr.sc_chains_per_period)
        include_segment_pl = bool(pr.include_segment_pl)

    seed = st.number_input("Random seed", min_value=0, value=42, step=1)

    st.subheader("Output")
    fmt = st.radio("Format", options=["delta", "parquet", "bigquery"], horizontal=True)
    if fmt == "bigquery":
        target = st.text_input(
            "BigQuery table (project.dataset.table)",
            value=os.environ.get("ACDOCA_BQ_TABLE", "my-gcp-project.synthetic_acdoca.journal_entries"),
        )
        gcs_temp_bucket = st.text_input(
            "GCS staging bucket (connector temp files)",
            value=os.environ.get("ACDOCA_GCS_TEMP_BUCKET", ""),
            help="Required for BigQuery writes. Same as ACDOCA_GCS_TEMP_BUCKET.",
        )
        parquet_path = None
    else:
        target = st.text_input(
            "Target table (catalog.schema.table for metastore-backed Delta)",
            value="synthetic.acdoca.journal_entries",
        )
        parquet_path = None
        if fmt == "parquet":
            parquet_path = st.text_input(
                "Parquet path (local directory or cloud URI)",
                value="/tmp/acdoca_synthetic",
            )
        gcs_temp_bucket = None

    if st.button("Generate", type="primary"):
        spark = _spark(for_bigquery=(fmt == "bigquery"))
        if preset_choice == "custom":
            ic_val = None if use_industry_ic else float(ic_pct) / 100.0
        else:
            ic_val = ic_pct
        cfg = GenerationConfig(
            industry_key=industry,
            country_isos=selected_isos,
            fiscal_year=int(fiscal_year),
            fiscal_variant=str(fiscal_variant),
            complexity=str(complexity),
            txn_per_cc_per_period=int(txn_per),
            ic_pct=ic_val,
            include_reversals=bool(rev),
            include_closing=bool(closing),
            seed=int(seed),
            include_supply_chain=bool(include_supply_chain),
            sc_chains_per_period=int(sc_chains_per_period),
            include_segment_pl=bool(include_segment_pl),
        )
        bar = st.progress(0.0, text="Generating…")
        try:
            gen_result = generate_acdoca_dataframe(spark, cfg)
            df = gen_result.acdoca_df
            bar.progress(0.5, text="Validating…")
            results = run_validations(df, profile=validation_profile)
            fails = blocking_failures(results)
            bar.progress(0.75, text="Writing…")
            meta_industry = canonical_industry_key(industry)
            if not fails:
                write_acdoca_table(
                    spark,
                    df,
                    full_table_name=target,
                    gen=GenerationParams(
                        industry=meta_industry,
                        complexity=str(complexity),
                        countries_iso_csv=",".join(selected_isos),
                        fiscal_year=int(fiscal_year),
                        seed=int(seed),
                        validation_profile=validation_profile,
                    ),
                    output_format=fmt,
                    parquet_path=parquet_path,
                    gcs_temp_bucket=gcs_temp_bucket,
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
            if fmt == "delta":
                st.success(f"Written to **{target}**")
            elif fmt == "parquet":
                st.success(f"Parquet written to **{parquet_path}**")
            else:
                st.success(f"BigQuery write complete: **{target}**")

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

        if gen_result.supply_chain_flows_df is not None:
            st.subheader("Supply chain flows (hops)")
            sc_df = gen_result.supply_chain_flows_df
            n_sc = sc_df.count()
            st.metric("Supply chain hop rows", f"{n_sc:,}")
            st.dataframe(sc_df.limit(200).toPandas(), use_container_width=True)
            try:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix="_sc_flows.json")
                tmp.close()
                export_supply_chain_json(sc_df, tmp.name)
                st.info(
                    "Open the **Dash** graph in a **separate terminal** (install `pip install -e \".[viz]\"`), "
                    "leave it running, then open the URL:\n\n"
                    f"`python -m acdoca_generator.dash_app.app --data {tmp.name}`\n\n"
                    "**http://127.0.0.1:8050/** — Error **-102 / connection refused** means nothing is listening: "
                    "the server must stay running in that terminal (or **--port 8051** if 8050 is in use)."
                )
            except Exception as ex:  # noqa: BLE001
                st.warning(f"Could not export supply chain JSON: {ex}")

        if gen_result.segment_pl_df is not None:
            st.subheader("Segment P&L")
            pl_df = gen_result.segment_pl_df
            n_pl = pl_df.count()
            st.metric("P&L rows", f"{n_pl:,}")
            st.dataframe(pl_df.limit(200).toPandas(), use_container_width=True)


if __name__ == "__main__":
    main()
