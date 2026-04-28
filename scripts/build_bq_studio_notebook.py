#!/usr/bin/env python3
"""Rewrite ``notebooks/02_generate_acdoca_bq_studio.ipynb`` so BigQuery Studio
renders it as an app surface — markdown headers + the form widget visible,
code cells collapsed by default.

Updates:
  - Cell 0 (intro) is rewritten with a "how to use" guide and the obsolete
    "default runtime is insufficient" caveat removed (Phase A made larger
    presets reliable).
  - Cell 8 (form) is augmented with: an Estimate-rows button (no-Spark dry run),
    a live result panel (BQ console link, balance check, 5-row sample), and a
    Schema-preview accordion driven by FIELD_DESCRIPTIONS.
  - Every code cell gets ``cellView: "form"`` (Colab/BQ Studio) and
    ``jupyter.source_hidden: true`` (JupyterLab) so the source is hidden by
    default; user clicks the cell to expand if debugging.

Idempotent: re-running overwrites the notebook with the same content. The
current notebook structure (15 cells, 6 markdown sections) is preserved.
"""

from __future__ import annotations

import json
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_NB = _REPO / "notebooks" / "02_generate_acdoca_bq_studio.ipynb"


_INTRO_MD = """# ACDOCA Synthetic Generator — BigQuery Studio app

Interactive form for the synthetic SAP ACDOCA generator. Fill in the widgets, click **Generate**, and the output lands in a BigQuery table — partitioned, clustered, with SAP field descriptions on every column.

## How to use

1. **Open in BigQuery Studio.** Click the *Run all* button at the top of the notebook (the cells below are collapsed by default — click any cell header to expand source for inspection or debugging).
2. **Edit project + bucket** in *Configure* below — set your GCP project id, BigQuery dataset, and GCS staging bucket. Defaults reflect Greg's project; replace them with your own.
3. **Pick a preset** in the form, or switch to *custom* and tune the widgets. The **Estimate rows** button gives you a quick row-count without running anything.
4. **Click Generate.** Spark generation typically completes in 5–60 s on the default Colab runtime; the result panel below shows row count, balance check, a link to the BQ console, and a 5-row sample.
5. **Run the SQL cells below** for additional slices (period totals, transaction-type mix, year-end trueup audit).

## Prerequisites (one-time GCP setup)

- GCP project with the BigQuery + Cloud Storage + Dataproc APIs enabled
- A BigQuery dataset (e.g. `synthetic_acdoca`) — see [`notebooks/00_bq_setup.sql`](./00_bq_setup.sql)
- A GCS bucket for the Spark BigQuery connector's staging files
- IAM: `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`, `roles/dataproc.editor`, plus `objectAdmin` on the staging bucket (or `roles/owner` for the whole project)

## Notes

- **Repo source** — this notebook clones `https://github.com/GDAVIDREEVES/bigquery-sap-synthetic-data.git` (public). Push your local changes first (`git push origin main`) so the notebook gets the latest code on the next *Run all*.
- **GCS connector** — needed by the Spark BigQuery connector to stage temp files via `gs://`. The setup cell downloads the shaded JAR (zero transitive deps).
"""


_FORM_CELL = '''import ipywidgets as W
import pandas as pd
from IPython.display import display, clear_output, HTML

from acdoca_generator.config._field_descriptions_generated import FIELD_DESCRIPTIONS
from acdoca_generator.config._field_specs_generated import FIELD_SPECS

_SUPPORTED_ISOS = [
    "US", "DE", "GB", "FR", "IE", "CH", "IN", "CN", "JP",
    "BR", "MX", "NL", "SG", "KR", "AU", "IT", "ES", "BE",
    "DK", "CA", "IL", "PL", "PH", "CR", "ID", "RU",
]

# ---------- core inputs ----------
preset_dd = W.Dropdown(
    options=["custom"] + preset_keys(), value="quick_smoke", description="Preset",
    description_tooltip="Pick a named scenario; the rest of the form snaps to its values. Switch to 'custom' to edit each widget freely.",
)
industry_dd = W.Dropdown(
    options=industry_keys(), value="pharmaceutical", description="Industry",
    description_tooltip="Drives margin bands, IC share, transaction-type mix, and supply-chain templates.",
)
country_mc = W.SelectMultiple(
    options=_SUPPORTED_ISOS, value=("US", "DE", "CH", "IE"), description="Countries", rows=8,
    description_tooltip="Each ISO becomes one company code in the run. ≥ 2 are required for IC + supply chain.",
)
year_slider = W.IntSlider(
    value=2026, min=2020, max=2030, description="Year",
    description_tooltip="Fiscal year for every generated document.",
)
fiscal_variant_rb = W.RadioButtons(
    options=["calendar", "april"], value="calendar", description="Fiscal",
    description_tooltip="K4 = calendar (Jan-Dec); V3 = April-March.",
)
complexity_rb = W.RadioButtons(
    options=["light", "medium", "high", "very_high"], value="medium", description="Tier",
    description_tooltip="Field-coverage tier: light ≈ 50 fields populated, very_high ≈ all 538.",
)
txn_slider = W.IntSlider(
    value=200, min=50, max=2000, step=50, description="Txn/CC/period",
    description_tooltip="Domestic transactions per company code per posting period (12 periods/year).",
    style={"description_width": "initial"},
)
ic_pct_slider = W.FloatSlider(
    value=0.30, min=0.0, max=0.6, step=0.05, description="IC %",
    description_tooltip="Fraction of total lines that go through intercompany pairing.",
    readout_format=".2f",
)
use_industry_ic = W.Checkbox(
    value=False, description="Use industry-default IC % (overrides slider)",
    description_tooltip="Tick to ignore the slider and use the industry template's default.",
    style={"description_width": "initial"},
)
include_reversals = W.Checkbox(
    value=True, description="Reversals (~5%)",
    description_tooltip="Tag ~5% of domestic docs as reversal originals (XREVERSING='X').",
)
include_closing = W.Checkbox(
    value=True, description="Closing entries",
    description_tooltip="Add 7 closing-step balanced docs per (company, period) — month-end style postings.",
)
include_sc = W.Checkbox(
    value=True, description="Supply chain",
    description_tooltip="Add multi-hop financial supply-chain flows (TP methods, markup bands, controversy tagging).",
)
sc_chains_slider = W.IntSlider(
    value=50, min=1, max=500, step=10, description="SC chains",
    description_tooltip="Number of distinct supply-chain flows. Each chain produces 3-4 hops, each hop = 4 IC lines.",
)
include_segment_pl = W.Checkbox(
    value=True, description="Segment P&L",
    description_tooltip="Build the segment-level P&L aggregation alongside the journal entries.",
)
include_trueup = W.Checkbox(
    value=True, description="Year-end trueup",
    description_tooltip="Q4 manual journal that brings each LRD's operating margin into its target band.",
)
challenged_slider = W.FloatSlider(
    value=0.0, min=0.0, max=0.5, step=0.05, description="Challenged %",
    description_tooltip="Fraction of supply-chain flows tagged as challenged by a tax authority (controversy/APA scenarios).",
    readout_format=".2f",
    style={"description_width": "initial"},
)
seed_int = W.IntText(
    value=42, description="Seed",
    description_tooltip="Deterministic seed; same value → same data.",
)

# ---------- buttons + outputs ----------
estimate_btn = W.Button(description="Estimate rows", icon="calculator")
go_btn = W.Button(description="Generate", button_style="primary", icon="play")
estimate_out = W.HTML(value="")
out = W.Output(layout={"border": "1px solid #ddd", "padding": "6px", "min_height": "120px"})
result_panel = W.Output(layout={"border": "1px solid #cfe", "padding": "8px", "margin_top": "8px"})

_ALL_INPUTS = [
    industry_dd, country_mc, year_slider, fiscal_variant_rb,
    complexity_rb, txn_slider, ic_pct_slider, use_industry_ic,
    include_reversals, include_closing, include_sc, sc_chains_slider,
    include_segment_pl, include_trueup, challenged_slider, seed_int,
]


def _apply_preset(_change):
    """Snap the rest of the form to the selected preset's values."""
    key = preset_dd.value
    if key == "custom":
        return
    p = get_preset(key)
    industry_dd.value = p.industry_key
    isos = tuple(c.strip() for c in p.country_isos_csv.split(",") if c.strip())
    country_mc.value = tuple(i for i in isos if i in _SUPPORTED_ISOS)
    year_slider.value = p.fiscal_year
    fiscal_variant_rb.value = p.fiscal_variant
    complexity_rb.value = p.complexity
    txn_slider.value = min(max(p.txn_per_cc_per_period, txn_slider.min), txn_slider.max)
    if p.ic_pct is None:
        use_industry_ic.value = True
    else:
        use_industry_ic.value = False
        ic_pct_slider.value = p.ic_pct
    include_reversals.value = p.include_reversals
    include_closing.value = p.include_closing
    include_sc.value = p.include_supply_chain
    sc_chains_slider.value = p.sc_chains_per_period
    include_segment_pl.value = p.include_segment_pl
    challenged_slider.value = float(getattr(p, "challenged_share", 0.0))


preset_dd.observe(_apply_preset, names="value")
_apply_preset(None)


def _row_estimate() -> dict:
    """Quick row-count estimate without touching Spark.

    Mirrors the formula in pipeline.py: domestic + IC + closing + SC + trueup.
    Numbers are approximate but within ~10% of the actual write count.
    """
    n_comp = len(country_mc.value)
    if n_comp == 0:
        return {"total": 0, "domestic": 0, "ic": 0, "closing": 0, "sc": 0, "trueup": 0}
    total_lines = max(0, n_comp * 12 * int(txn_slider.value))
    total_lines = (total_lines // 2) * 2
    ic_share = 0.0 if n_comp < 2 else (
        float(ic_pct_slider.value) if not use_industry_ic.value else 0.30
    )
    ic_lines = int(round(total_lines * ic_share / 4.0)) * 4
    ic_lines = min(ic_lines, total_lines)
    domestic_lines = (total_lines - ic_lines) // 2 * 2
    closing_rows = 7 * 12 * n_comp * 2 if include_closing.value else 0
    # SC chains → ~3.5 steps avg → 4 lines each ≈ 14/chain
    sc_rows = int(int(sc_chains_slider.value) * 14) if (include_sc.value and n_comp >= 2) else 0
    trueup_rows = 4 * n_comp if (include_trueup.value and n_comp >= 2) else 0
    total = domestic_lines + ic_lines + closing_rows + sc_rows + trueup_rows
    return {
        "total": total, "domestic": domestic_lines, "ic": ic_lines,
        "closing": closing_rows, "sc": sc_rows, "trueup": trueup_rows,
    }


def _on_estimate(_btn):
    e = _row_estimate()
    estimate_out.value = (
        f"<div style='padding:6px; background:#f7f7f7; border-radius:4px;'>"
        f"≈ <strong>{e['total']:,}</strong> rows: "
        f"domestic {e['domestic']:,} · IC {e['ic']:,} · "
        f"closing {e['closing']:,} · supply-chain {e['sc']:,} · trueup {e['trueup']:,}"
        f"</div>"
    )


estimate_btn.on_click(_on_estimate)


def _bq_console_url(table_id: str) -> str:
    project, dataset, table = table_id.split(".")
    return (
        f"https://console.cloud.google.com/bigquery"
        f"?project={project}&p={project}&d={dataset}&t={table}&page=table"
    )


def _render_result(cfg, table_id, df_spark):
    """Show row count, balance, BQ link, and a 5-row sample after a successful write."""
    with result_panel:
        clear_output()
        rows = df_spark.count()
        deb = float(df_spark.filter(F.col("DRCRK") == "S").agg(F.sum("WSL")).collect()[0][0] or 0)
        cred = float(df_spark.filter(F.col("DRCRK") == "H").agg(F.sum("WSL")).collect()[0][0] or 0)
        net = deb + cred
        url = _bq_console_url(table_id)
        balance_color = "#2c7" if abs(net) < 0.01 else "#c33"
        display(HTML(
            f"<h4 style='margin:0 0 6px 0;'>✓ Wrote {rows:,} rows to {table_id}</h4>"
            f"<ul style='margin:0 0 8px 18px;'>"
            f"<li><a href='{url}' target='_blank'>Open in BigQuery console</a></li>"
            f"<li>Balance: debits {deb:,.2f} + credits {cred:,.2f} = "
            f"<span style='color:{balance_color}'>net {net:,.2f}</span></li>"
            f"<li>Industry={cfg.industry_key} · countries={cfg.country_isos} · "
            f"year={cfg.fiscal_year} · complexity={cfg.complexity} · seed={cfg.seed}</li>"
            f"</ul>"
            f"<p style='margin:0 0 4px 0;'><em>Sample 5 rows from BigQuery:</em></p>"
        ))
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        sample = client.query(
            f"SELECT BELNR, RBUKRS, RACCT, DRCRK, WSL, BUDAT, AWREF, SGTXT "
            f"FROM `{table_id}` LIMIT 5"
        ).to_dataframe()
        display(sample)


def _on_generate(_btn):
    with out:
        clear_output()
        result_panel.clear_output()
        for w in _ALL_INPUTS + [preset_dd, go_btn, estimate_btn]:
            w.disabled = True
        try:
            country_isos = list(country_mc.value)
            if not country_isos:
                print("Pick at least one country.")
                return
            cfg = GenerationConfig(
                industry_key=industry_dd.value,
                country_isos=country_isos,
                fiscal_year=int(year_slider.value),
                fiscal_variant=fiscal_variant_rb.value,
                complexity=complexity_rb.value,
                txn_per_cc_per_period=int(txn_slider.value),
                include_reversals=bool(include_reversals.value),
                include_closing=bool(include_closing.value),
                seed=int(seed_int.value),
                ic_pct=None if use_industry_ic.value else float(ic_pct_slider.value),
                include_supply_chain=bool(include_sc.value),
                sc_chains_per_period=int(sc_chains_slider.value),
                include_segment_pl=bool(include_segment_pl.value),
                include_year_end_trueup=bool(include_trueup.value),
                challenged_share=float(challenged_slider.value),
            )
            print(f"Generating: industry={cfg.industry_key} countries={country_isos} "
                  f"year={cfg.fiscal_year} complexity={cfg.complexity} "
                  f"txn/cc/period={cfg.txn_per_cc_per_period} seed={cfg.seed}")
            result = generate_acdoca_dataframe(spark, cfg)
            print("Generation complete. Writing to BigQuery ...")
            gen_params = GenerationParams(
                industry=cfg.industry_key,
                complexity=cfg.complexity,
                countries_iso_csv=",".join(country_isos),
                fiscal_year=cfg.fiscal_year,
                seed=cfg.seed,
                validation_profile="strict",
            )
            table_id = os.environ["ACDOCA_BQ_TABLE"]
            write_acdoca_table(
                spark, result.acdoca_df, full_table_name=table_id, gen=gen_params,
                output_format="bigquery", gcs_temp_bucket=os.environ["ACDOCA_GCS_TEMP_BUCKET"],
            )
            _render_result(cfg, table_id, result.acdoca_df)
        except Exception as e:
            print(f"FAILED: {type(e).__name__}: {e}")
            raise
        finally:
            for w in _ALL_INPUTS + [preset_dd, go_btn, estimate_btn]:
                w.disabled = False


go_btn.on_click(_on_generate)


# ---------- schema preview accordion ----------
def _build_schema_html() -> str:
    rows = []
    for sap, sql, tier, stype in FIELD_SPECS:
        desc = FIELD_DESCRIPTIONS.get(sap, "")
        rows.append({"SAP": sap, "BQ column": sql, "Tier": tier, "Type": stype, "Description": desc})
    df = pd.DataFrame(rows)
    return df.to_html(index=False, classes="acdoca-schema", border=0)


schema_acc = W.Accordion(
    children=[W.HTML(value=_build_schema_html())],
    selected_index=None,
)
schema_acc.set_title(0, f"⛯ Schema reference — all {len(FIELD_SPECS)} fields with descriptions (click to expand)")


# ---------- layout ----------
form = W.VBox([
    W.HTML("<h3 style='margin:0 0 6px 0;'>ACDOCA generator parameters</h3>"),
    preset_dd,
    W.HBox([industry_dd, year_slider, fiscal_variant_rb]),
    country_mc,
    W.HBox([complexity_rb, txn_slider]),
    W.HBox([ic_pct_slider, use_industry_ic]),
    W.HBox([include_reversals, include_closing]),
    W.HBox([include_sc, sc_chains_slider]),
    W.HBox([include_segment_pl, include_trueup]),
    challenged_slider,
    seed_int,
    W.HBox([estimate_btn, go_btn]),
    estimate_out,
    W.HTML("<h4 style='margin:8px 0 4px 0;'>Generation log</h4>"),
    out,
    W.HTML("<h4 style='margin:8px 0 4px 0;'>Result</h4>"),
    result_panel,
    schema_acc,
])
display(form)
'''


# Cells 10 + 13 originally aliased COUNT(*) AS rows. ``rows`` is a reserved
# keyword in BigQuery GoogleSQL (used by the VALUES ROWS(...) inline-array
# syntax) and unaliased uses fail with a parse error at the SELECT clause.
# Rewriting the alias to ``n_rows`` avoids the conflict.
_VERIFY_CELL = '''from google.cloud import bigquery

client = bigquery.Client(project=PROJECT_ID)
df = client.query(f"""
    SELECT COUNT(*) AS n_rows,
           MIN(GJAHR) AS y_min,
           MAX(GJAHR) AS y_max,
           COUNT(DISTINCT RBUKRS) AS n_company_codes
    FROM `{FULL_BQ_TABLE}`
""").to_dataframe()
df
'''

_FLOWTYPE_CELL = '''client.query(f"""
    SELECT SUBSTR(AWREF, 1, 2) AS prefix, COUNT(*) AS n_rows
    FROM `{FULL_BQ_TABLE}`
    WHERE AWREF IS NOT NULL AND AWREF != ''
    GROUP BY prefix
    ORDER BY prefix
""").to_dataframe()
'''


def main() -> int:
    nb = json.loads(_NB.read_text())

    # 1. Replace cell 0 (intro)
    nb["cells"][0]["source"] = _INTRO_MD.splitlines(keepends=True)

    # 2. Replace cell 8 (form)
    nb["cells"][8]["source"] = _FORM_CELL.splitlines(keepends=True)

    # 3. Replace cells 10 + 13 (BigQuery verify + flow-type SQL — drop reserved-keyword alias)
    nb["cells"][10]["source"] = _VERIFY_CELL.splitlines(keepends=True)
    nb["cells"][13]["source"] = _FLOWTYPE_CELL.splitlines(keepends=True)

    # 4. Hide source on every code cell (Colab + JupyterLab)
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        meta = cell.setdefault("metadata", {})
        meta["cellView"] = "form"
        meta.setdefault("jupyter", {})["source_hidden"] = True
        # Clear stale exec output so the saved notebook stays small
        cell["outputs"] = []
        cell["execution_count"] = None

    _NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n")
    print(f"Updated {_NB.relative_to(_REPO)} ({len(nb['cells'])} cells)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
