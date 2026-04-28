# Supply chain realism review — 2026-04-27

> **Status update (2026-04-27 same-day):** All workstream-A and workstream-B
> items from this diagnostic are now addressed. See commit `bd61e51`
> (non-goods IC flows + year-end true-up + controversy/APA + entity roles).
> Items still open: Missing #4 (CbCR/Pillar Two — workstream C, deferred)
> and Missing #7 / Bugs #3 (Polars fast path for IC + SC — workstream D,
> explicitly deprioritized by project north star). Status table at the
> bottom of this file lists per-item resolution.


Smoke-tested the financial supply chain feature merged on 2026-04-05. Goal: confirm the
output is realistic enough to feed an **operational transfer pricing solution** built on
top of it. This is a TP-analyst lens, not a structural validator's lens.

## Run summary

- Config: `industry_key="pharmaceutical"`, 4 country codes (`US, DE, IE, CH`), `fiscal_year=2025`,
  `complexity="medium"`, `txn_per_cc_per_period=2`, `ic_pct=0.30`, `sc_chains_per_period=12`.
- Output: 240 ACDOCA rows, 36 supply chain flow rows (12 chains × 3 hops × 1 buyer/hop).
- Cross-border share: **36 / 36 = 100%** (every hop crosses a tax border, expected for IC chains).
- IC balance check: **all IC documents net to 0** ✓.
- Dash viewer renders 4 nodes (one per BUKRS) and 36 edges.

## What looks right

1. **Three-step pharma chain shape is plausible.** The single template produces TOLL → IPPR → FRMF → LRD
   with materials API-001 (RAW) → DPR-001 (SEMI) → final goods. This matches the textbook
   principal-structure pattern for pharma multinationals (toll mfg → IP principal → final
   formulation → distributors). [`acdoca_generator/config/supply_chain_templates.py`](../acdoca_generator/config/supply_chain_templates.py)
2. **TP method choice tracks role pair convention.** The ROLE_TP_METHOD mapping at
   [`acdoca_generator/config/tp_methods.py:17-35`](../acdoca_generator/config/tp_methods.py#L17-L35)
   uses Cost-Plus for routine mfg/services, CUP for IP-related transfers, RPM for distributor
   resale, PSM for HQ-to-LRD splits, TNMM for financing. That's how a real TP study would
   document method selection.
3. **Markup ranges are defensible per role pair.** TOLL→IPPR cost-plus 3–5%, RDSC→IPPR
   6–10%, RPM with negative markups (-4% to -2%) for IP→distributor flows, profit split
   0–3% for HQ→LRD. These are inside published benchmarking ranges for routine functions.
4. **IC documents are doc-balanced.** Every BELNR's WSL nets to 0 across the 4 paired rows
   (buyer inventory/AP_IC, seller AR_IC/revenue_IC) per [`supply_chain.py:_four_ic_rows_for_hop`](../acdoca_generator/generators/supply_chain.py).
   This is table-stakes correctness and it holds.
5. **Cross-border tax country pairs are exposed.** Every flow has SELLER_LAND1 / BUYER_LAND1
   columns populated, which is what a jurisdictional analysis needs as input.
6. **Material lineage carries through the chain.** Each step has its own MATNR (API → DPR →
   FG), with USAGE_FACTOR and STANDARD_COST progressing realistically (raw 150 → semi 320 →
   FG presumably higher). Operational TP cares about cost-stack progression for benchmarking.
7. **AWREF / AWTYP / AWORG are populated.** The supply chain IC rows carry distinct
   document references (`AWREF=SC...`), `AWTYP=BKPF`, `AWSYS=SYNTH_SC`. That's the kind of
   anchor an operational TP tool needs to thread events back to source documents.
8. **WERKS (plant) is populated on the goods leg only.** Lines 1+3 of each IC doc (the
   inventory and AR sides) carry the seller's plant code; AP/revenue lines do not. Mirrors
   real S/4HANA where plant only attaches to material movements.

## What's weak (would need work before TP product-readiness)

1. **Only one supply-chain template per industry.** [`supply_chain_templates_for_industry`](../acdoca_generator/config/supply_chain_templates.py)
   returns a single 3-step template for every industry. With `n_chains=12` you get 12
   identical-shape chains. A real pharma principal has dozens of distinct SKU lineages,
   parallel R&D-licensing arms, and contract-mfg subchains. **Output today does not
   exercise SKU- or chain-level diversity.**
2. **Single buyer per hop.** Default branching is one buyer per hop unless `dest_role="LRD"`
   ([`supply_chain.py:461-470`](../acdoca_generator/generators/supply_chain.py#L461-L470)).
   Real principal structures fan out (one CMFR sells to 5+ FRMFs; one IPPR licenses to
   regional principals globally). This understates IC volume relative to entity count.
3. **Period clumping.** `_poper_i(seed, chain_i, step_i)` produces a single POPER per (chain, step).
   In our run, all 12 chain-step-1 rows landed on POPER 007. Real chains generate flows
   monthly across a fiscal year. Operational TP analytics that test monthly margin
   trends, year-end true-ups, or quarterly seasonality won't have signal here.
4. **No TP method variation within a role pair.** ROLE_TP_METHOD is a fixed dict — every
   IPPR→FRMF flow uses CUP. In practice, the same role pair can use different methods for
   different transaction types (tangible goods vs. services vs. royalties). The product
   would need that nuance to support method-selection workflows.
5. **Markup band randomness is uniform within `[lo, hi]`.** Real benchmarking ranges show
   skew (median is rarely the midpoint). The flat distribution overstates how clean
   real-world margin data is.
6. **TOLL flows ignore industry context.** The default 3-step template uses TOLL → IPPR
   → FRMF → LRD for *every* industry, including media and technology where toll mfg is
   uncommon. Industry-conditional templates would help.
7. **No service charges, royalties, or cost-share recharges.** Operationally, IC service
   fees, IT recharges, R&D cost shares, and royalty flows account for a major share of
   IC volume. None of those are in the supply chain module today (or in the rest of the
   pipeline as far as this smoke test reached).
8. **`include_supply_chain` is off by default.** The Streamlit UI may or may not surface
   the toggle. If TP-realism is the project north star, the supply-chain pathway should
   probably default on for any run with `n_companies >= 2`.

## What's missing for an operational TP solution (as opposed to "structurally valid SAP data")

1. **Segment-level P&Ls.** Operational TP needs to compute LRD operating margin, principal
   residual, and routine returns separately. ACDOCA's SEGMENT field is populated but no
   segment-aware aggregation logic exists in the generator.
2. **Year-end true-up adjustments.** Real LRDs are squeezed/credited at year-end to land
   on a target margin. No mechanism here. This is the single most common operational TP
   workflow and the data shape needed (Q4 reversing journal at the LRD entity, paired
   with a year-end IC manual JE) is not produced.
3. **Royalty / IP licensing flows.** No rows tagged as royalty payments, no royalty rate
   schedules, no separate stream for IP-residual catch-up.
4. **CbCR / Pillar Two-relevant aggregation.** The data is at journal grain, but a TP
   product needs revenue / profit / tax / employees / tangible assets per jurisdiction.
   No employee headcount, no asset book values, no tax accruals are modeled (or if they
   are elsewhere, they aren't surfaced through the supply chain module).
5. **Master-file structure metadata.** Which entity is the principal? Which holds IP?
   Which is the financing arm? `IP_OWNER` and `RESIDUAL_PROFIT_OWNER` are on the flow
   schema, but downstream consumers can't discover the principal directly from ACDOCA
   alone.
6. **Controversy / dispute scenarios.** No "challenged transaction" markers, no
   pre-agreed APA flows with conservative pricing, no scenarios that mimic a tax
   authority's adjusted view of a flow.
7. **Volume realism.** 12 chains/period generating 36 flows is fine for smoke testing.
   A real pharma multinational does 1000s of IC documents/month. Generation throughput
   on a 4-country, ~250-row run took ~7 minutes wall-clock, dominated by Spark startup
   plus `cidx.collect()` materialization. Scaling to TP-realistic volume needs a
   different data-volume strategy (likely the Polars fast path extended to IC + supply
   chain).

## Bugs / friction encountered

1. **PySpark 3.5.8 `toPandas()` is broken on Python 3.12.** [`supply_chain.py:556`](../acdoca_generator/generators/supply_chain.py#L556)
   calls `flows_df.toPandas()`, which transitively imports `from distutils.version import LooseVersion`.
   `distutils` was removed from the stdlib in Python 3.12. The smoke script bypassed this
   with a direct `Row.asDict() → json.dump` fallback, but the in-repo `export_supply_chain_flows_json`
   helper will fail for any user on Python 3.12. **Suggested fix:** rewrite `export_supply_chain_flows_json`
   to avoid `toPandas()` (use `toJSON().collect()` or `Row.asDict()` walk), or pin the
   project to Python 3.11 in `pyproject.toml` (currently `>=3.10`).
2. **PySpark startup needs ≥ 4g driver heap for even tiny workloads.** Default `1g` OOM'd
   on a 240-row generation. The OOM was in the executor side during shuffle. The CI
   workflows already share one Spark session across tests for this reason. Running locally
   for the first time is friction-heavy.
3. **`master("local[2]")` + `cidx.collect()` is the bottleneck.** [`pipeline.py:52`](../acdoca_generator/generators/pipeline.py#L52)
   collects companies to driver. Cheap on its own, but the pipeline collects + recreates
   DataFrames several times. For tiny smoke runs this is overhead-dominated.
4. **`PYSPARK_PYTHON` must be explicitly set** to the venv Python or workers spawn the
   system Python (3.9 here) and fail with PYTHON_VERSION_MISMATCH. Worth surfacing in
   the README dev-install section.

## Verdict

For a feature that landed three weeks ago, the supply chain module **works end-to-end**:
generation, IC pairing, JSON export (with a one-line workaround), and the Dash viewer all
function. The data shape is structurally correct and TP-method assignments per role pair
are sensible.

The realism gap relative to **operational TP product needs** is large, mostly in three
directions: (1) a single template per industry produces low-diversity output, (2) period
distribution is clumpy, and (3) the operational TP workstreams that aren't goods-flow —
royalties, service charges, year-end true-ups, segment P&Ls, CbCR aggregation — aren't
modeled at all.

None of this is a regression — it's a feature scope question. Decide whether the next
investment is:
- **(a)** broaden chain templates and roles per industry (low-risk, high-impact),
- **(b)** fix the `distutils` bug in the in-repo export helper (1 line, do it now), or
- **(c)** start a new module for non-goods IC flows (royalties, services, true-ups).

## Files touched this session

- Created [`scripts/dev_smoke_supply_chain.py`](../scripts/dev_smoke_supply_chain.py)
  (throwaway dev runner — safe to delete or keep)
- Created `/tmp/sc_flows.json` (smoke output, not committed)
- Created `.venv-plan/` (gitignored already per [.gitignore](../.gitignore))
- This file: [`diagnostics/sc-realism-2026-04-27.md`](sc-realism-2026-04-27.md)

No production code modified.

---

## Resolution status (2026-04-27 end-of-day)

| Section | # | Item | Status | Evidence |
|---|---|---|---|---|
| Weak | 1 | One template per industry | ✅ Done (prior) | 2 templates per industry; pharma now 5 (+royalty/service/cost-share/APA) |
| Weak | 2 | Single buyer per hop | ✅ Done (prior) | `fanout_all` flag |
| Weak | 3 | Period clumping | ✅ Done (prior) | chain_start_poper anchored, monotonic step periods |
| Weak | 4 | No TP method variation per role pair | ✅ Done | `ROLE_TP_METHOD` re-keyed to `(seller, buyer, txn_type)` with goods fallback |
| Weak | 5 | Uniform markup distribution | ✅ Done (prior) | Triangular distribution |
| Weak | 6 | TOLL ignores industry context | ✅ Done (prior) | Per-industry templates |
| Weak | 7 | No royalty/service/cost-share flows | ✅ Done | New `transaction_type` discriminator + `LegSpec` factory; AWREF prefixes RY/SV/CS |
| Weak | 8 | `include_supply_chain` off by default | ✅ Done | Default flipped to True (`quick_smoke` opts out) |
| Missing | 1 | Segment-level P&Ls | ✅ Done (prior) | `aggregations/segment_pl.py` |
| Missing | 2 | Year-end true-up adjustments | ✅ Done | New `generators/year_end_trueup.py`; Q4 paired JEs at LRD↔IPPR with BLART="SA", AWREF "TU" |
| Missing | 3 | Royalty / IP licensing flows | ✅ Done | `transaction_type="royalty"` + `revenue_royalty` GL; lands in `other_income` segment_pl bucket |
| Missing | 4 | CbCR / Pillar Two aggregation | 🔲 Out of scope | Deferred to workstream C |
| Missing | 5 | Master-file structure metadata | ✅ Done | New `aggregations/entity_roles.py` exposes IS_PRINCIPAL / IS_IP_OWNER / IS_FINANCING_ARM / etc. flags |
| Missing | 6 | Controversy / APA scenarios | ✅ Done | `apa_flag`+`apa_markup_band` on SupplyChainStep; APA_FLAG/CHALLENGED_FLAG/ADJUSTED_VIEW_PRICE on flow schema; `controversy_demo` preset; `challenged_share` config |
| Missing | 7 | Volume realism / Polars IC+SC | 🔲 Out of scope | Deferred to workstream D (north star deprioritizes performance over scenario realism) |
| Bugs | 1 | Py3.12 `toPandas` crash | ✅ Done (prior) | `Row.asDict()` fallback in export helper |
| Bugs | 2 | Spark heap default friction | ✅ Done | README "Local Spark gotchas" section documents `SPARK_DRIVER_MEMORY=4g` |
| Bugs | 3 | `cidx.collect()` bottleneck | 🔲 Out of scope | Deferred to workstream D |
| Bugs | 4 | `PYSPARK_PYTHON` not surfaced | ✅ Done | README "Local Spark gotchas" documents the env var |
