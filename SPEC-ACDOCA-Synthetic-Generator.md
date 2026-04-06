# ACDOCA Synthetic Data Generator — Technical Specification

**Version:** 1.0  
**Date:** March 31, 2026  
**Target Platform:** PySpark 3.5+, Google BigQuery (Spark connector), local Streamlit UI; optional Polars fast path  
**Build Tool:** Claude Code  

---

## 1. Purpose

Build a PySpark-based application that generates realistic synthetic SAP S/4HANA ACDOCA (Universal Journal) data. The generated data will be used for transfer pricing analysis, financial supply chain modeling, intercompany transaction testing, Pillar Two / GloBE readiness, and AI/ML demo pipelines. The generator must produce structurally valid, internally consistent journal entries that reflect real-world industry operating models and multinational entity structures.

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                  Streamlit UI (local PySpark)             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────┐ │
│  │ Industry  │ │Countries │ │ Fiscal   │ │ Complexity  │ │
│  │ Template  │ │ Selector │ │ Year     │ │ Tier        │ │
│  └─────┬────┘ └─────┬────┘ └────┬─────┘ └──────┬──────┘ │
│        └────────────┴───────────┴───────────────┘        │
│                          │                                │
│              ┌───────────▼────────────┐                   │
│              │  Generation Orchestrator│                   │
│              └───────────┬────────────┘                   │
│    ┌─────────────────────┼─────────────────────┐         │
│    ▼                     ▼                     ▼         │
│ ┌──────────┐    ┌──────────────┐    ┌────────────────┐   │
│ │ Master   │    │  Transaction │    │  IC Transaction│   │
│ │ Data Gen │    │  Engine      │    │  Engine        │   │
│ └──────────┘    └──────────────┘    └────────────────┘   │
│                          │                                │
│              ┌───────────▼────────────┐                   │
│              │  Validation & Balance  │                   │
│              │  Checker               │                   │
│              └───────────┬────────────┘                   │
│                          ▼                                │
│         Delta / Parquet (path or metastore) or BigQuery    │
│              (project.dataset.table)                       │
└──────────────────────────────────────────────────────────┘
```

### 2.1 Module Inventory

| Module | File | Responsibility |
|--------|------|----------------|
| `app.py` | Streamlit entry point | UI, parameter collection, orchestration |
| `config/industries.py` | Industry template definitions | CoA, entity structures, IC patterns |
| `config/countries.py` | Country reference data | Currency, tax codes, fiscal year variants |
| `config/field_tiers.py` | Complexity tier field lists | Which fields populate at each tier |
| `config/operating_models.py` | TP entity archetypes | Principal, LRD, CM, etc. role definitions |
| `generators/master_data.py` | Master data factory | Company codes, cost centers, profit centers, materials, vendors, customers |
| `generators/transactions.py` | Transaction engine | Domestic journal entries by type |
| `generators/intercompany.py` | IC transaction engine | Paired IC entries with TP attributes |
| `generators/amounts.py` | Currency & amount logic | Multi-currency translation, FX rates |
| `generators/document.py` | Document numbering | BELNR, DOCLN, AWTYP/AWREF sequencing |
| `validators/balance.py` | Validation suite | Debit = Credit per doc, IC reconciliation |
| `utils/spark_writer.py` | Spark writer | Schema enforcement; Delta / Parquet / BigQuery sinks |

---

## 3. Streamlit User Interface

### 3.1 Layout

The UI should present a single-page form with the following sections, top to bottom:

**Section A — Industry Template** (radio buttons, single select)
- Pharmaceutical
- Medical Device
- Consumer Goods
- Technology
- Media

**Section B — Countries** (multi-select checkboxes with "Select All")
- United States, China, Germany, Switzerland, Japan, India, France, United Kingdom, Ireland, Brazil, Canada, Belgium, Italy, South Korea, Spain, Netherlands, Denmark, Singapore, Israel, Australia

**Section C — Fiscal Year** (dropdown, default = current year)
- Range: 2020–2030
- Option: Calendar year (Jan–Dec) or April fiscal year (Apr–Mar)

**Section D — Complexity Tier** (radio buttons with inline summary)
- Light — ~55 fields — Core financial postings
- Medium — ~130 fields — TP & segmented reporting
- High — ~250 fields — Full cost accounting & CO-PA
- Very High — ~400+ fields — Enterprise simulation

**Section E — Volume Controls**
- Transactions per company code per period: slider (100 – 50,000; default 1,000)
- Intercompany transaction percentage: slider (5% – 60%; default 25%)
- Include reversals: checkbox (default on, ~5% reversal rate)
- Include closing entries: checkbox (default on)
- Random seed: numeric input (for reproducibility)

**Section F — Output**
- Target catalog.schema path (text input, default `synthetic.acdoca.journal_entries`)
- Format: Delta table (default), Parquet export option
- "Generate" button → progress bar → summary statistics on completion

### 3.2 Post-Generation Summary Panel

After generation completes, display:
- Row count by company code
- Total debit / credit balance (should be zero per document)
- IC transaction count and paired validation pass/fail
- Field coverage count (X of 538 fields populated)
- Sample preview (first 100 rows in a data grid)

---

## 4. Complexity Tiers — Field Mapping

The tiers are designed for financial, transfer pricing, and financial supply chain use cases. Each tier is cumulative (Medium includes all Light fields, etc.).

### 4.1 LIGHT — Core Financial Posting (~55 fields)

**Use case:** Basic trial balance, simple GL analysis, data pipeline testing.

**What you get:** Every journal entry has document identity, one company code, one profit center, posting date, account, amounts in transaction + local + group currency, and basic document metadata. Enough to build a TB and do simple IC identification.

| Category | Fields |
|----------|--------|
| **Primary Key** | `RCLNT`, `RLDNR`, `RBUKRS`, `GJAHR`, `BELNR`, `DOCLN` |
| **Record Classification** | `RRCTY`, `RMVCT`, `BLART`, `BSTAT` |
| **Core Org Structure** | `KOKRS`, `RACCT`, `RCNTR`, `PRCTR`, `SEGMENT` |
| **Partner ID (IC)** | `RASSC`, `PPRCTR`, `PBUKRS` |
| **Amounts — Transaction Ccy** | `RWCUR`, `WSL`, `DRCRK` |
| **Amounts — Local Ccy** | `RHCUR`, `HSL` |
| **Amounts — Group Ccy** | `RKCUR`, `KSL` |
| **Dates** | `BUDAT`, `BLDAT`, `POPER`, `GJAHR`, `FISCYEARPER`, `PERIV` |
| **Document Metadata** | `RYEAR`, `DOCNR_LD`, `BUZEI`, `BSCHL`, `KOART`, `USNAM`, `TIMESTAMP` |
| **Account Type** | `GLACCOUNT_TYPE`, `KTOPL` |
| **Quantity (basic)** | `RUNIT`, `MSL` |
| **Counterparty** | `LIFNR`, `KUNNR` |
| **Text** | `SGTXT` |
| **Country** | `LAND1`, `TAX_COUNTRY` |

**Total: ~55 fields**

---

### 4.2 MEDIUM — Transfer Pricing & Segmented Reporting (~130 fields)

**Use case:** Intercompany analysis, segmented P&L, Pillar Two entity mapping, TP documentation data, basic supply chain traceability.

**Adds to Light:**

| Category | Fields Added |
|----------|-------------|
| **Full Document Flow** | `AWTYP`, `AWSYS`, `AWORG`, `AWREF`, `AWITEM`, `AWITGRP` |
| **Reversals** | `XREVERSING`, `XREVERSED`, `XTRUEREV`, `AWTYP_REV`, `AWORG_REV`, `AWREF_REV`, `AWITEM_REV` |
| **Preceding Document** | `PREC_AWTYP`, `PREC_AWORG`, `PREC_AWREF`, `PREC_AWITEM`, `PREC_BUKRS`, `PREC_GJAHR`, `PREC_BELNR`, `PREC_DOCLN` |
| **Extended Org** | `RFAREA`, `RBUSA`, `SCNTR`, `SFAREA`, `SBUSA`, `PSEGMENT`, `EPRCTR` |
| **Multi-Currency (Parallel)** | `RTCUR`, `TSL`, `ROCUR`, `OSL` |
| **Profit Center Valuation** | `WSL3` |
| **Functional Currency** | `RFCCUR`, `FCSL` |
| **Consolidation** | `RBUNIT`, `RBUPTR`, `RCOMP`, `RITCLG`, `RITEM` |
| **Supply Chain** | `MATNR`, `WERKS`, `MATKL_MM`, `EBELN`, `EBELP`, `KDAUF`, `KDPOS` |
| **Tax** | `MWSKZ`, `UMSKZ` |
| **Clearing** | `XOPVW`, `AUGDT`, `AUGBL`, `AUGGJ` |
| **Payment Terms** | `NETDT`, `VALUT` |
| **Transaction Keys** | `KTOSL`, `LINETYPE`, `VORGN`, `VRGNG`, `BTTYPE` |
| **Secondary Entry Flag** | `XSECONDARY`, `XSETTLING`, `XSETTLED` |
| **SD Fields** | `FKART`, `VKORG`, `VTWEG`, `SPART` |
| **Revenue Recognition** | `FBUDA`, `PEROP_BEG`, `PEROP_END`, `RA_CONTRACT_ID`, `RA_POB_ID` |
| **Closing** | `CLOSINGSTEP`, `CLOSING_RUN_ID` |
| **Migration** | `SDM_VERSION` |
| **Profit Center Derivation** | `PRCTR_DRVTN_SOURCE_TYPE` |
| **Offsetting** | `GKONT`, `GKOAR` |
| **Assignment** | `ZUONR`, `REBZG`, `REBZJ`, `REBZZ` |

**Total: ~130 fields**

---

### 4.3 HIGH — Full Cost Accounting, CO-PA & Material Ledger (~250 fields)

**Use case:** Full TP documentation with cost-plus and TNMM benchmarking support, product costing variance analysis, CO-PA multi-dimensional profitability, Material Ledger actual costing, asset accounting integration, activity-based costing.

**Adds to Medium:**

| Category | Fields Added |
|----------|-------------|
| **CO Objects** | `OBJNR`, `ACCAS`, `ACCASTY`, `LSTAR`, `AUFNR`, `AUTYP`, `PRZNR`, `KSTRG`, `BEMOT`, `SCOPE`, `LOGSYSO` |
| **CO Partner Objects** | `PAROB1`, `PAROBSRC`, `PACCAS`, `PACCASTY`, `PLSTAR`, `PAUFNR`, `PAUTYP`, `PSCOPE`, `LOGSYSP` |
| **CO Document** | `CO_BELNR`, `CO_BUZEI`, `CO_ZLENR`, `CO_BELKZ`, `CO_BEKNZ`, `BELTP` |
| **CO Origin** | `HRKFT`, `HKGRP`, `OBJNR_HK`, `AUFNR_ORG`, `UKOSTL`, `ULSTAR`, `UPRZNR`, `UPRCTR`, `UMATNR`, `VARC_UACCT`, `USPOB` |
| **CO Allocation** | `BWSTRAT` |
| **CO Statistical** | `CO_ACCASTY_N1`, `CO_ACCASTY_N2`, `CO_ACCASTY_N3` |
| **CO Quantities** | `CO_MEINH`, `CO_MEGBTR`, `CO_MEFBTR`, `RVUNIT`, `VMSL`, `VMFSL`, `RRUNIT`, `RMSL`, `RMSL_TYPE`, `MFSL` |
| **Fixed/Variable Split** | `KFSL`, `KFSL2`, `KFSL3` |
| **Variance Fields** | `PSL`, `PSL2`, `PSL3`, `PFSL`, `PFSL2`, `PFSL3` |
| **CO Currency** | `RCO_OCUR`, `CO_OSL` |
| **CO-PA Core** | `ERKRS`, `PAOBJNR`, `XPAOBJNR_CO_REL`, `PPAOBJNR` |
| **CO-PA Characteristics (selected)** | `MATNR_COPA`, `MATKL`, `KDGRP`, `BRSCH`, `BZIRK`, `KUNRE`, `KUNWE`, `KONZS`, `VKGRP_PA`, `VKBUR_PA`, `KMBRND_PA`, `KMKDGR_PA`, `KMLAND_PA`, `KMMAKL_PA`, `PAPH1_PA`, `PRODH_PA`, `PAPH2_PA`, `PAPH3_PA`, `REGIO_PA`, `PARTNER_PA`, `KMSTGE_PA`, `WW0RE_PA`, `WW0SB_PA` |
| **Material Ledger** | `VPRSV`, `MLAST`, `KALNR`, `BWTAR`, `BWKEY`, `MLPTYP`, `MLCATEG`, `MLPOSNR`, `INV_MOV_CATEG`, `QSBVALT`, `QSPROCESS`, `PERART` |
| **Inventory Valuation** | `HPVPRS`, `KPVPRS`, `HSTPRS`, `KSTPRS`, `HPEINH`, `KPEINH`, `HSALK3`, `KSALK3`, `LBKUM`, `SOBKZ` |
| **Asset Accounting** | `AFABE`, `ANLN1`, `ANLN2`, `BZDAT`, `ANBWA`, `MOVCAT`, `DEPR_PERIOD`, `ANLKL`, `KTOGR` |
| **Project System** | `PS_PSP_PNR`, `PS_POSID`, `PS_PRJ_PNR`, `PS_PSPID` |
| **Partner Projects** | `PPS_PSP_PNR`, `PPS_POSID`, `PKDAUF`, `PKDPOS` |
| **HR** | `PERNR` |
| **Parallel Currencies 3-4** | `RVCUR`, `VSL`, `RBCUR`, `BSL` |
| **Alternative Values** | `HSLALT`, `KSLALT` |
| **External Values** | `HSLEXT`, `KSLEXT` |
| **Additional Quantities** | `QUNIT1`, `QUANT1`, `RIUNIT` |
| **Document Splitting** | `SUBTA`, `XSPLITMOD` |
| **Purchasing** | `ZEKKN`, `EBELN_LOGSYS`, `RBEST` |
| **Special GL** | `UMSKZ` |

**Total: ~250 fields**

---

### 4.4 VERY HIGH — Enterprise Simulation (~400+ fields)

**Use case:** Full-fidelity SAP simulation for demo environments, AI training data, migration testing. Includes all industry-specific, JVA, real estate, grants/funds management, accrual engine, and the full CO-PA characteristic set.

**Adds to High:**

| Category | Fields Added |
|----------|-------------|
| **All Remaining Currencies** | `RCCUR` through `RGCUR`, `CSL` through `GSL` |
| **All Alternative Values** | `OSLALT` through `GSLALT` |
| **All External Values** | `OSLEXT` through `GSLEXT` |
| **All Inventory Valuations** | `OPVPRS`, `VPVPRS`, `OSTPRS`, `VSTPRS`, `OSALK3`, `VSALK3`, `HSALKV`, `KSALKV`, `OSALKV`, `VSALKV`, `OPEINH`, `VPEINH`, `HVKWRT`, `HVKSAL` |
| **Group Valuation Amounts** | `WSL2`, `KFSL2`, `KFSL3`, `PSL2`, `PSL3`, `PFSL2`, `PFSL3` |
| **All CO Valuation Views** | `CO_BUZEI1` through `CO_BUZEI7`, `CO_REFBZ` through `CO_REFBZ7` |
| **Source Document Chain** | `SRC_AWTYP`, `SRC_AWSYS`, `SRC_AWORG`, `SRC_AWREF`, `SRC_AWITEM`, `SRC_AWSUBIT` |
| **Full Reversal Chain** | `SUBTA_REV`, `PREC_AWSYS`, `PREC_SUBTA`, `PREC_AWMULT` |
| **Commitment** | `XCOMMITMENT`, `OBS_REASON` |
| **Special Stock** | `KZBWS`, `XOBEW`, `VTSTAMP`, `MAT_KDAUF`, `MAT_KDPOS`, `MAT_PSPNR`, `MAT_PS_POSID`, `MAT_LIFNR` |
| **Full CO-PA Characteristics** | All `_PA` fields: `MAABC_PA`, `BONUS_PA`, `EFORM_PA`, `GEBIE_PA`, `KMVKBU_PA`, `KMVKGR_PA`, `CRMELEM_PA`, `KMVTNR_PA`, `WWTES_PA`, `KMHI01_PA`, `KMHI02_PA`, `KMHI03_PA`, `CRMCSTY_PA`, `KMATYP_PA`, `KMDEST_PA`, `KMFLTN_PA`, `KMFLTY_PA`, `KMIATA_PA`, `KMLEGS_PA`, `KMOPDY_PA`, `KMORIG_PA`, `KMROUT_PA`, `KMWNHG_PA`, `KMZONE_PA`, `WWACT_PA`, `WWERM_PA`, `COLLE_PA`, `CRMFIGR_PA`, `KMCATG_PA`, `KMNIEL_PA`, `SAISJ_PA`, `SAISO_PA`, `ARTNRG_PA`, `MATNR_PA`, `WW001_PA`, `WW100_PA`, `WW101_PA`, `WWBU1_PA`, `ACDOC_COPA_EEW_DUMMY_PA` |
| **Asset Accounting (extended)** | `ANLGR`, `ANLGR2`, `SETTLEMENT_RULE`, `PANL1`, `PANL2`, `UBZDT_PN`, `XVABG_PN`, `PROZS_PN`, `XMANPROPVAL_PN`, `ANLN2_PN`, `BWASL_PN`, `BZDAT_PN`, `ANBTR_PN` |
| **Plant Maintenance** | `EQUNR`, `TPLNR`, `ISTRU`, `ILART`, `PLKNZ`, `ARTPR`, `PRIOK`, `MAUFNR`, `VORNR`, `AUFPS`, `UVORN`, `ARBID`, `WORK_ITEM_ID`, `OVERTIMECAT`, `PAUFPS`, `PLANNED_PARTS_WORK` |
| **Service Management** | `SERVICE_DOC_TYPE`, `SERVICE_DOC_ID`, `SERVICE_DOC_ITEM_ID`, `SERVICE_CONTRACT_TYPE`, `SERVICE_CONTRACT_ID`, `SERVICE_CONTRACT_ITEM_ID`, `SOLUTION_ORDER_ID`, `SOLUTION_ORDER_ITEM_ID`, `PSERVICE_DOC_TYPE`, `PSERVICE_DOC_ID`, `PSERVICE_DOC_ITEM_ID` |
| **Network / PS extended** | `NPLNR`, `NPLNR_VORGN`, `PNPLNR`, `PNPLNR_VORGN`, `PPRZNR`, `PKSTRG`, `RSRCE`, `QMNUM` |
| **Funds Management** | `FIKRS`, `FIPEX`, `FISTL`, `MEASURE`, `RFUND`, `RGRANT_NBR`, `RBUDGET_PD`, `SFUND`, `SGRANT_NBR`, `SBUDGET_PD` |
| **Budget** | `BDGT_ACCOUNT` through `BDGT_CNSMPN_AMOUNT_TYPE` |
| **Grants** | `RSPONSORED_PROG`, `RSPONSORED_CLASS`, `RBDGT_VLDTY_NBR`, `RGM_OCUR`, `GM_OSL` |
| **Earmarked Funds** | `KBLNR`, `KBLPOS` |
| **Joint Venture** | `VNAME`, `EGRUP`, `RECID`, `VPTNR`, `BTYPE`, `ETYPE`, `PRODPER`, `BILLM`, `POM`, `CBRUNID`, `PVNAME`, `PEGRUP`, `S_RECIND`, `CBRACCT`, `CBOBJNR`, `JVACTIVITY` |
| **Real Estate** | `SWENR`, `SGENR`, `SGRNR`, `SMENR`, `RECNNR`, `SNKSL`, `SEMPSL`, `DABRZ`, `PSWENR`, `PSGENR`, `PSGRNR`, `PSMENR`, `PRECNNR`, `PSNKSL`, `PSEMPSL`, `PDABRZ` |
| **Cash Management (RE)** | `RE_BUKRS`, `RE_ACCOUNT` |
| **Accrual Engine** | `ACROBJTYPE`, `ACRLOGSYS`, `ACROBJ_ID`, `ACRSOBJ_ID`, `ACRITMTYPE`, `ACRVALDAT`, `ACRREFOBJ_ID` |
| **Financial Valuation** | `VALOBJTYPE`, `VALOBJ_ID`, `VALSOBJ_ID` |
| **Consolidation (extended)** | `SITYP`, `SUBIT` |
| **Migration** | `MIG_SOURCE`, `MIG_DOCLN` |
| **Risk / Follow-up** | `RISK_CLASS`, `FUP_ACTION` |
| **Data Aging** | `_DATAAGING` |
| **Additional Quantities** | `QUNIT2`, `QUNIT3`, `QUANT2`, `QUANT3` |
| **Remaining** | `LOKKT`, `KTOP2`, `REBZT`, `COCO_NUM`, `WWERT`, `HBKID`, `HKTID`, `RHOART`, `ORGL_CHANGE`, `ERLKZ`, `MUVFLG`, `ZMATNR`, `VTKEY`, `VTPOS`, `SLALITTYPE` |

**Total: ~400+ fields (all 538 minus ~30 technical include markers and true dummies)**

---

## 5. Industry Templates

Each industry template defines a complete synthetic enterprise: legal entities, org structure, chart of accounts skeleton, intercompany transaction types, and realistic operating model roles.

### 5.1 Template Structure (applies to all industries)

```python
class IndustryTemplate:
    name: str                          # e.g. "pharmaceutical"
    chart_of_accounts: dict            # account ranges by category
    entity_roles: list[EntityRole]     # operating model roles
    ic_transaction_types: list[ICTxn]  # intercompany transaction catalog
    product_hierarchy: dict            # product groups, SKUs
    cost_center_structure: dict        # functional cost center templates
    profit_center_structure: dict      # by entity role
    transaction_mix: dict              # % split by transaction type
    seasonality: dict                  # monthly volume weights (1.0 = flat)
    typical_margins: dict              # by entity role, for TP-realistic amounts
```

### 5.2 Chart of Accounts — Universal Structure

All industries share the same account numbering framework. Industry-specific sub-ranges are defined within.

| Range | Category | Examples |
|-------|----------|----------|
| 0010000–0019999 | Cash & Banks | 0010000 Cash, 0011000 Bank Operating |
| 0100000–0199999 | Receivables | 0100000 Trade AR, 0110000 IC Receivables |
| 0200000–0299999 | Inventory | 0200000 Raw Materials, 0210000 WIP, 0220000 Finished Goods |
| 0300000–0399999 | Fixed Assets | 0300000 Land, 0310000 Buildings, 0320000 Machinery |
| 0400000–0499999 | Other Assets | 0400000 Prepaid, 0410000 Intangibles, 0420000 Goodwill |
| 0500000–0599999 | Payables | 0500000 Trade AP, 0510000 IC Payables, 0520000 Accrued Liab |
| 0600000–0699999 | Other Liabilities | 0600000 Loans, 0610000 Tax Payable, 0620000 Deferred Rev |
| 0700000–0799999 | Equity | 0700000 Share Capital, 0710000 Retained Earnings |
| 0800000–0849999 | Revenue | 0800000 Product Rev, 0810000 Service Rev, 0820000 IC Rev |
| 0850000–0899999 | Other Income | 0850000 FX Gains, 0860000 Interest Income |
| 0900000–0919999 | COGS | 0900000 COGS Product, 0910000 IC COGS |
| 0920000–0949999 | Operating Expenses | 0920000 Personnel, 0925000 R&D, 0930000 SGA, 0935000 Marketing |
| 0950000–0969999 | IC Charges | 0950000 Mgmt Fee Expense, 0955000 Royalty Expense, 0960000 Service Fee Exp |
| 0970000–0989999 | Depreciation & Amort | 0970000 Depreciation, 0975000 Amortization |
| 0990000–0999999 | Tax & Extraordinary | 0990000 Income Tax, 0995000 Extraordinary Items |
| Secondary (9-series) | CO Secondary Elements | 9100000 Activity Alloc, 9200000 Assessment, 9300000 Settlement |

### 5.3 Industry — Pharmaceutical

**Operating Model Archetype:**

| Entity Role | Code | Description | Typical Countries | Target Margin |
|-------------|------|-------------|-------------------|---------------|
| IP Principal | IPPR | Owns IP, bears R&D risk, earns residual | US, CH, IE | Residual (30–45% OM) |
| Full-Risk Manufacturer | FRMF | Owns mfg IP, bears inventory risk | US, DE, JP | 15–25% OM |
| Contract Manufacturer | CMFR | Mfg under contract, limited risk | IN, IE, SG, CN | Cost + 5–8% |
| Toll Manufacturer | TOLL | Converts materials, no inventory risk | IN, CN, BR | Cost + 3–5% |
| Limited Risk Distributor | LRD | Buys/resells, limited market risk | DE, FR, UK, IT, ES, NL, KR, AU, BE, DK | 2–4% OM on sales |
| Commissionaire | COMM | Sells on behalf of principal, no title risk | FR, IT, BE | 2–3% commission on sales |
| R&D Service Center | RDSC | Contract R&D services | IN, CN, IL | Cost + 8–12% |
| Shared Service Center | SSC | Finance, IT, HR services | IN, PL, MY | Cost + 5–8% |
| Regional HQ / Mgmt Co | RHQ | Management, strategic oversight | CH, SG, UK, US | Cost + 5–10% |
| Financing Entity | FINC | IC lending, cash pooling | IE, NL, CH, LU | Arm's-length interest spread |

**IC Transaction Types:**

| Transaction | Debit Entity | Credit Entity | AWTYP | Typical Accounts |
|-------------|-------------|---------------|-------|-----------------|
| Finished Goods Sale (IP → LRD) | LRD (Inventory) | IPPR (IC Revenue) | BKPF | 0200000 / 0820000 |
| Contract Mfg Service | IPPR (COGS) | CMFR (IC Rev) | BKPF | 0910000 / 0820000 |
| Toll Mfg Service | CMFR (COGS) | TOLL (IC Rev) | BKPF | 0910000 / 0820000 |
| R&D Service Fee | IPPR (R&D Exp) | RDSC (IC Rev) | BKPF | 0925000 / 0810000 |
| Mgmt Fee Charge | All subs (Exp) | RHQ (IC Rev) | BKPF | 0950000 / 0810000 |
| Royalty / License Fee | All mfg (Exp) | IPPR (IC Rev) | BKPF | 0955000 / 0850000 |
| Shared Service Fee | All entities (Exp) | SSC (IC Rev) | BKPF | 0960000 / 0810000 |
| IC Loan Interest | Borrower (Int Exp) | FINC (Int Inc) | BKPF | 0600000 / 0860000 |

**Pharma-Specific Product Hierarchy:**

```
PH01: Innovative Drugs
  PH0101: Oncology
  PH0102: Immunology
  PH0103: CNS / Neuroscience
  PH0104: Cardiovascular
PH02: Biosimilars
  PH0201: Monoclonal Antibodies
  PH0202: Insulin Analogs
PH03: Generics
  PH0301: Oral Solids
  PH0302: Injectables
PH04: Consumer Health / OTC
  PH0401: Pain Relief
  PH0402: Vitamins & Supplements
```

**Transaction Mix (% of total entries):**

| Type | % |
|------|---|
| Third-party revenue | 25% |
| Procurement / COGS | 20% |
| Intercompany goods | 15% |
| Intercompany services | 10% |
| Payroll / Personnel | 12% |
| R&D expenses | 8% |
| Depreciation / Amort | 3% |
| Tax postings | 3% |
| Other (FX, accruals, etc.) | 4% |

---

### 5.4 Industry — Medical Device

**Differences from Pharma:**

- Higher proportion of contract manufacturing (implants, instruments require specialized mfg)
- Consignment stock model is common (SOBKZ = 'K') — devices placed in hospitals
- Loaner/demo inventory tracked as assets (ANLN1)
- Field service and instrument refurbishment cost flows
- Product hierarchy: Surgical Instruments, Implants (Ortho, Cardiac, Neuro), Diagnostics, Capital Equipment
- Regulatory costs are a larger expense category
- Typical LRD margins: 2–5% OM (slightly higher than pharma due to service component)

**Additional IC Transactions:**
- Consignment replenishment (IPPR → LRD, special stock indicator SOBKZ = 'K')
- Instrument refurbishment services (SSC/CMFR → distributor)
- Regulatory filing fee charges (RHQ → local entities)

---

### 5.5 Industry — Consumer Goods

**Differences from Pharma:**

- Higher third-party revenue volume, lower IC %
- More distribution entities, fewer R&D entities
- Heavy seasonal patterns (configurable: Q4 holiday peak for general; summer peak for beverages)
- Significant marketing and trade promotion spending
- Product hierarchy: Food & Beverage, Personal Care, Home Care, Baby Care, Pet Care
- Commissionaire model common in Europe (FR, IT, BE)
- CO-PA deeply used for brand/channel/region profitability
- Typical LRD margins: 1.5–3.5% OM

**Additional IC Transactions:**
- Marketing cost-sharing / reimbursement (LRD → IPPR)
- Trade promotion fund allocations
- Brand royalty charges (higher rate than pharma: 3–6% of net sales)

---

### 5.6 Industry — Technology

**Differences from Pharma:**

- IP licensing and royalties are the dominant IC flow (royalty rates 8–15%+ of revenue)
- Software revenue recognition (IFRS 15 / ASC 606) drives heavy use of RA_CONTRACT_ID and RA_POB_ID
- Cloud/SaaS subscription revenue is recognized over time → accrual engine fields used
- R&D expense is the largest opex category (25–40% of revenue)
- Product hierarchy: Enterprise Software, Cloud/SaaS, Hardware, Semiconductor, IT Services
- Operating model emphasizes IP holding in low-tax jurisdiction (IE, SG) with cost-sharing arrangements
- Shared services are large (engineering centers in IN, IL, CN)
- Typical LRD margins: 1–3% OM on resale; service entities at cost + 8–12%

**Additional IC Transactions:**
- Software license / IP royalty (operating entities → IP holdco)
- Cost-sharing R&D contributions (multiple entities → IPPR)
- Cloud hosting cost recharges (infrastructure entity → all)
- Hardware component procurement (mfg in CN/KR → distribution entities)

---

### 5.7 Industry — Media

**Differences from Pharma:**

- Content licensing rights as the dominant IC transaction
- Revenue streams: advertising, subscriptions, content licensing, live events
- Complex revenue share arrangements (CO-PA critical for content profitability)
- Production costs capitalized as assets (content library = intangible asset)
- Product hierarchy: Streaming, Advertising, Publishing, Live Events, Licensing
- Operating model: Content Production Hub, Regional Distribution, Ad Sales entities
- Significant prepaid and deferred revenue patterns
- Typical distributor margins: 2–4% OM; content production entities at cost + 6–10%

**Additional IC Transactions:**
- Content license fees (distributor → content hub)
- Advertising revenue share (ad sales entity ↔ platform entity)
- Production service fees (production hub → regional entities)
- Technology platform charges (tech entity → all)

---

## 6. Country Configuration

### 6.1 Country Reference Data

| Country | ISO | Currency | FY Variant (Cal) | FY Variant (Apr) | Typical Tax Code Prefix | Sample Company Code |
|---------|-----|----------|-------------------|-------------------|------------------------|-------------------|
| United States | US | USD | K4 | V3 | US | 1000 |
| China | CN | CNY | K4 | — | CN | 2000 |
| Germany | DE | EUR | K4 | — | DE | 3000 |
| Switzerland | CH | CHF | K4 | — | CH | 3100 |
| Japan | JP | JPY | K4 | V6 | JP | 4000 |
| India | IN | INR | K4 | V3 | IN | 4100 |
| France | FR | EUR | K4 | — | FR | 3200 |
| United Kingdom | UK | GBP | K4 | V9 | GB | 3300 |
| Ireland | IE | EUR | K4 | — | IE | 3400 |
| Brazil | BR | BRL | K4 | — | BR | 5000 |
| Canada | CA | CAD | K4 | — | CA | 1100 |
| Belgium | BE | EUR | K4 | — | BE | 3500 |
| Italy | IT | EUR | K4 | — | IT | 3600 |
| South Korea | KR | KRW | K4 | — | KR | 4200 |
| Spain | ES | EUR | K4 | — | ES | 3700 |
| Netherlands | NL | EUR | K4 | — | NL | 3800 |
| Denmark | DK | DKK | K4 | — | DK | 3900 |
| Singapore | SG | SGD | K4 | — | SG | 4300 |
| Israel | IL | ILS | K4 | — | IL | 4400 |
| Australia | AU | AUD | K4 | V3 | AU | 6000 |

### 6.2 Company Code Assignment Logic

When a user selects countries, the generator creates one or more company codes per country depending on the industry template. The entity role assignment follows the mapping in section 5.x for the selected industry. If a country is not mapped to any entity role in the template, it defaults to LRD.

**Naming Convention:**
- Company Code: 4-digit (from table above, +1 for second entity in same country)
- Profit Center: `{CC}{ROLE}01` (e.g., `1000IPPR01` for US IP Principal)
- Cost Center: `{CC}{FUNC}01` where FUNC = MFGR, RDEV, SALE, GADM, ITSV, etc.

### 6.3 Multi-Ledger Configuration

| Ledger | ID | Description | Currency |
|--------|----|-------------|----------|
| Leading Ledger | 0L | IFRS / Group Reporting | Group currency (USD for US-parented groups) |
| Non-Leading Ledger | 2L | Local GAAP | Local currency |

The generator always creates entries in the leading ledger (0L). At Medium complexity and above, local GAAP ledger (2L) entries are generated for entities in countries where local GAAP diverges materially (DE, JP, BR, CN, IN, FR, KR).

### 6.4 FX Rate Logic

The generator uses a built-in FX rate table with realistic mid-market rates (hardcoded baseline, not live). Rates are pegged to USD as the base. Monthly average rates are generated by applying a small random walk (±2% annual vol for major pairs, ±5% for EM pairs). The month-end rate and monthly average rate are both stored and applied:

- **HSL** (local currency): translated at posting-date spot rate
- **KSL** (group currency): translated at posting-date spot rate
- **Period-end revaluation entries**: generated at period-end closing for open items using month-end rate vs. original rate

---

## 7. Transaction Generation Logic

### 7.1 Document Structure Rules

Every generated accounting document must satisfy:

1. **Balanced entry:** Sum of all debits = sum of all credits within the same document (BELNR + RBUKRS + GJAHR)
2. **Consistent document header:** All lines in a document share BUDAT, BLDAT, BLART, BSTAT
3. **Sequential DOCLN:** Line items numbered sequentially within each document (000001, 000002, ...)
4. **Valid posting keys (BSCHL):** 40 = GL Debit, 50 = GL Credit, 01 = Customer Debit (invoice), 11 = Customer Credit, 21 = Vendor Debit, 31 = Vendor Credit (invoice), 70 = Asset Debit, 75 = Asset Credit
5. **DRCRK consistency:** 'S' for debit, 'H' for credit — must match BSCHL and amount sign
6. **Period alignment:** POPER derived from BUDAT per PERIV rules. FISCYEARPER = GJAHR * 1000 + POPER

### 7.2 Document Types (BLART)

| BLART | Description | Generated For |
|-------|-------------|---------------|
| SA | GL Posting | Manual journals, accruals, reclasses |
| RE | Invoice – Incoming | Vendor invoices (procurement) |
| DR | Customer Invoice | Third-party revenue |
| DZ | Customer Payment | Cash receipts |
| KZ | Vendor Payment | Cash disbursements |
| AB | Accounting Document | IC settlements, allocations |
| AA | Asset Posting | Depreciation, acquisitions, retirements |
| WE | Goods Receipt | Inventory receipts (AWTYP = MKPF) |
| WA | Goods Issue | Cost of goods sold postings |
| RV | Billing Document | SD billing (AWTYP = VBRK) |

### 7.3 Intercompany Transaction Pairing

Every IC transaction generates **two documents** — one in each company code — with matching amounts:

```
Document 1 (Buyer Entity):               Document 2 (Seller Entity):
  Line 1: DR Expense/Inventory              Line 1: DR IC Receivable (KUNNR = buyer)
          RASSC = Seller CC                          RASSC = Buyer CC
          PPRCTR = Seller PC                         PPRCTR = Buyer PC
  Line 2: CR IC Payable (LIFNR = seller)     Line 2: CR IC Revenue
          RASSC = Seller CC                          RASSC = Buyer CC
          PPRCTR = Seller PC                         PPRCTR = Buyer PC
```

**IC Reconciliation Keys:**
- Same AWREF on both sides (shared IC document reference)
- Mirrored RASSC / PBUKRS
- Amounts equal in transaction currency (RWCUR + WSL)
- KSL amounts may differ due to FX translation differences (realistic)

### 7.4 Period-End Closing Entries

When "Include closing entries" is enabled, the generator creates for each company code at each period end:

| Closing Step | CLOSINGSTEP | Description |
|--------------|-------------|-------------|
| Foreign currency reval | 010 | Revalue open AP/AR at month-end rate |
| GR/IR clearing | 020 | Clear matched GR/IR items |
| Depreciation run | 030 | Monthly asset depreciation |
| Accrual reversal | 040 | Reverse prior-period accruals |
| Accrual posting | 050 | Post current-period accruals |
| CO allocation | 060 | Distribute shared service costs |
| IC elimination (group) | 070 | Elimination entries in group ledger |

### 7.5 Amount Generation Strategy

Amounts are generated to produce realistic financial statements per entity role:

1. **Start from revenue:** Each third-party selling entity gets a total annual revenue target based on country GDP weight × industry size factor
2. **Derive COGS:** Apply industry-typical gross margin (pharma ~65–75%, tech ~60–70%, CPG ~40–50%, meddev ~55–65%, media ~45–55%)
3. **Derive opex by function:** R&D, SGA, G&A — percentages from industry template
4. **IC flows sized by TP policy:** LRD buy price = IPPR sell price = target LRD OM × LRD revenue; CMFR revenue = cost + markup
5. **Apply monthly distribution:** Flat ÷ 12 with seasonality weights from template
6. **Randomize within ±15%:** Individual transaction amounts vary around the target to avoid artificial uniformity

---

## 8. Validation Rules

The validation suite runs automatically after generation and reports results in the UI.

| Rule | Check | Severity |
|------|-------|----------|
| DOC_BALANCE | Sum(WSL) where DRCRK='S' = Sum(WSL) where DRCRK='H' per document | FAIL — blocks save |
| IC_PAIR | Every AWREF with RASSC ≠ '' has matching document in partner company code | WARN |
| IC_AMOUNT | Paired IC documents have equal WSL in same RWCUR | WARN |
| PERIOD_VALID | POPER ∈ {001–012, 013–016} and consistent with BUDAT | FAIL |
| PK_UNIQUE | No duplicate (RCLNT, RLDNR, RBUKRS, GJAHR, BELNR, DOCLN) | FAIL |
| DRCRK_SIGN | DRCRK = 'S' when WSL > 0, 'H' when WSL < 0 (or vice versa per convention) | FAIL |
| CURRENCY_SET | RHCUR matches company code country currency | WARN |
| ACCOUNT_EXISTS | RACCT exists in configured chart of accounts | WARN |
| PRCTR_ASSIGNED | PRCTR is non-blank for all P&L postings | WARN |

---

## 9. Data Model & Storage

### 9.1 Delta Table Schema

The output table uses all 538 ACDOCA fields with proper Spark SQL types. Fields not populated at the selected complexity tier are present in the schema but contain NULL.

**Type Mapping:**

| SAP Type | Spark SQL Type |
|----------|---------------|
| CLNT, CHAR | StringType |
| NUMC | StringType (left-padded with zeros) |
| DATS | DateType |
| DEC | DecimalType(23, 2) |
| CURR | DecimalType(23, 2) |
| QUAN | DecimalType(23, 3) |
| CUKY | StringType |
| UNIT | StringType |
| RAW | StringType (hex-encoded) |

### 9.2 Partitioning

```sql
PARTITIONED BY (RBUKRS, GJAHR, POPER)
```

### 9.3 Table Properties

```sql
TBLPROPERTIES (
  'generator.version' = '1.0',
  'generator.industry' = '{selected_industry}',
  'generator.complexity' = '{selected_tier}',
  'generator.countries' = '{comma-separated ISO codes}',
  'generator.fiscal_year' = '{YYYY}',
  'generator.seed' = '{seed_value}',
  'generator.timestamp' = '{generation_utc_timestamp}'
)
```

---

## 10. Implementation Notes for Claude Code

### 10.1 Runtime environment

- Runtime: Python 3.10+, Spark 3.5+ (PySpark)
- UI: Streamlit locally; optional notebook hosts with widget APIs for parameters
- Storage: BigQuery via Spark connector + GCS staging, or Delta/Parquet to a path or metastore-backed table
- Compute: Single-node Spark for smaller volumes; cluster (e.g. Dataproc) for larger volumes
- No external API calls required — all generation logic is self-contained

### 10.2 Performance Targets

| Volume | Target Time |
|--------|-------------|
| 10K rows | < 10 seconds |
| 100K rows | < 60 seconds |
| 1M rows | < 5 minutes |
| 10M rows | < 30 minutes |

Use Spark DataFrame operations (not row-by-row Python loops). Generate master data as small DataFrames, then use `crossJoin` + `explode` + UDFs to produce transaction rows at scale.

### 10.3 File Organization

```
acdoca_generator/
├── app.py                          # Streamlit entry point
├── config/
│   ├── __init__.py
│   ├── industries.py               # All 5 industry templates
│   ├── countries.py                # Country reference data + FX rates
│   ├── field_tiers.py              # Field lists per complexity tier
│   ├── operating_models.py         # Entity role definitions + margins
│   └── chart_of_accounts.py        # Universal CoA + industry overrides
├── generators/
│   ├── __init__.py
│   ├── master_data.py              # Company codes, CCs, PCs, materials
│   ├── transactions.py             # Domestic transaction engine
│   ├── intercompany.py             # IC paired entry engine
│   ├── closing.py                  # Period-end closing entries
│   ├── amounts.py                  # Amount calculation + FX
│   └── document.py                 # Document numbering + sequencing
├── validators/
│   ├── __init__.py
│   └── balance.py                  # All validation rules
├── utils/
│   ├── __init__.py
│   └── spark_writer.py             # Delta writer + catalog registration
├── tests/
│   ├── test_balance.py
│   ├── test_ic_pairing.py
│   └── test_field_coverage.py
└── README.md
```

### 10.4 Key Design Principles

1. **Referential integrity over randomness:** Master data is generated first and all transactions reference valid master data values. No orphan cost centers, profit centers, or accounts.

2. **TP-realistic amounts:** IC transaction amounts should produce entity-level P&Ls with margins in the ranges specified per entity role. This is more important than individual transaction randomness.

3. **Document flow consistency:** AWTYP/AWREF chains should be traceable. A billing document (AWTYP = VBRK) should reference a delivery, which references a sales order.

4. **Deterministic with seed:** Same inputs + same seed = identical output. This enables reproducible demos and testing.

5. **Complexity tiers are strict subsets:** A field populated at Light is always populated at Medium, High, and Very High. No field "disappears" at a higher tier.

6. **NULL semantics:** Unpopulated fields are NULL (not empty string, not zero). This matches real SAP ACDOCA behavior where unused fields are initial/null.

---

## Appendix A: Field Tier Assignment — Complete List

The following lists every ACDOCA field and its assigned complexity tier. This is the authoritative reference for `config/field_tiers.py`.

**Legend:** L = Light, M = Medium, H = High, V = Very High, X = Excluded (technical includes / dummies)

| # | Field | Tier | | # | Field | Tier |
|---|-------|------|-|---|-------|------|
| 1 | RCLNT | L | | 2 | RLDNR | L |
| 3 | RBUKRS | L | | 4 | GJAHR | L |
| 5 | BELNR | L | | 6 | DOCLN | L |
| 7 | RYEAR | L | | 8 | DOCNR_LD | L |
| 9 | RRCTY | L | | 10 | RMVCT | L |
| 11 | VORGN | M | | 12 | VRGNG | M |
| 13 | BTTYPE | M | | 14 | CBTTYPE | H |
| 15 | AWTYP | M | | 16 | AWSYS | M |
| 17 | AWORG | M | | 18 | AWREF | M |
| 19 | AWITEM | M | | 20 | AWITGRP | M |
| 21 | SUBTA | H | | 22 | XREVERSING | M |
| 23 | XREVERSED | M | | 24 | XTRUEREV | M |
| 25 | AWTYP_REV | M | | 26 | AWORG_REV | M |
| 27 | AWREF_REV | M | | 28 | AWITEM_REV | M |
| 29 | SUBTA_REV | V | | 30 | XSETTLING | M |
| 31 | XSETTLED | M | | 32 | PREC_AWTYP | M |
| 33 | PREC_AWSYS | V | | 34 | PREC_AWORG | M |
| 35 | PREC_AWREF | M | | 36 | PREC_AWITEM | M |
| 37 | PREC_SUBTA | V | | 38 | PREC_AWMULT | V |
| 39 | PREC_BUKRS | M | | 40 | PREC_GJAHR | M |
| 41 | PREC_BELNR | M | | 42 | PREC_DOCLN | M |
| 43 | XSECONDARY | M | | 44 | CLOSING_RUN_ID | M |
| 45 | ORGL_CHANGE | V | | 46 | SRC_AWTYP | V |
| 47 | SRC_AWSYS | V | | 48 | SRC_AWORG | V |
| 49 | SRC_AWREF | V | | 50 | SRC_AWITEM | V |
| 51 | SRC_AWSUBIT | V | | 52 | XCOMMITMENT | V |
| 53 | OBS_REASON | V | | 54 | RTCUR | M |
| 55 | RWCUR | L | | 56 | RHCUR | L |
| 57 | RKCUR | L | | 58 | ROCUR | M |
| 59 | RVCUR | H | | 60 | RBCUR | H |
| 61 | RCCUR | V | | 62 | RDCUR | V |
| 63 | RECUR | V | | 64 | RFCUR | V |
| 65 | RGCUR | V | | 66 | RCO_OCUR | H |
| 67 | RGM_OCUR | V | | 68 | RUNIT | L |
| 69 | RVUNIT | H | | 70 | RRUNIT | H |
| 71 | RMSL_TYPE | H | | 72 | RIUNIT | H |
| 73 | QUNIT1 | H | | 74 | QUNIT2 | V |
| 75 | QUNIT3 | V | | 76 | CO_MEINH | H |
| 77 | RACCT | L | | 78 | RCNTR | L |
| 79 | PRCTR | L | | 80 | RFAREA | M |
| 81 | RBUSA | M | | 82 | KOKRS | L |
| 83 | SEGMENT | L | | 84 | SCNTR | M |
| 85 | PPRCTR | L | | 86 | SFAREA | M |
| 87 | SBUSA | M | | 88 | RASSC | L |
| 89 | PSEGMENT | M | | 90 | TSL | M |
| 91 | WSL | L | | 92 | WSL2 | V |
| 93 | WSL3 | M | | 94 | HSL | L |
| 95 | KSL | L | | 96 | OSL | M |
| 97 | VSL | H | | 98 | BSL | H |
| 99 | CSL | V | | 100 | DSL | V |
| 101 | ESL | V | | 102 | FSL | V |
| 103 | GSL | V | | 104 | KFSL | H |
| 105 | KFSL2 | V | | 106 | KFSL3 | V |
| 107 | PSL | H | | 108 | PSL2 | V |
| 109 | PSL3 | V | | 110 | PFSL | H |
| 111 | PFSL2 | V | | 112 | PFSL3 | V |
| 113 | CO_OSL | H | | 114 | GM_OSL | V |
| 115 | HSLALT | H | | 116 | KSLALT | H |
| 117 | OSLALT | V | | 118 | VSLALT | V |
| 119 | BSLALT | V | | 120 | CSLALT | V |
| 121 | DSLALT | V | | 122 | ESLALT | V |
| 123 | FSLALT | V | | 124 | GSLALT | V |
| 125 | HSLEXT | H | | 126 | KSLEXT | H |
| 127 | OSLEXT | V | | 128 | VSLEXT | V |
| 129 | BSLEXT | V | | 130 | CSLEXT | V |
| 131 | DSLEXT | V | | 132 | ESLEXT | V |
| 133 | FSLEXT | V | | 134 | GSLEXT | V |
| 135 | HVKWRT | V | | 136 | MSL | L |
| 137 | MFSL | H | | 138 | VMSL | H |
| 139 | VMFSL | H | | 140 | RMSL | H |
| 141 | QUANT1 | H | | 142 | QUANT2 | V |
| 143 | QUANT3 | V | | 144 | CO_MEGBTR | H |
| 145 | CO_MEFBTR | H | | 146 | HSALK3 | H |
| 147 | KSALK3 | H | | 148 | OSALK3 | V |
| 149 | VSALK3 | V | | 150 | HSALKV | V |
| 151 | KSALKV | V | | 152 | OSALKV | V |
| 153 | VSALKV | V | | 154 | HPVPRS | H |
| 155 | KPVPRS | H | | 156 | OPVPRS | V |
| 157 | VPVPRS | V | | 158 | HSTPRS | H |
| 159 | KSTPRS | H | | 160 | OSTPRS | V |
| 161 | VSTPRS | V | | 162 | HVKSAL | V |
| 163 | LBKUM | H | | 164 | DRCRK | L |
| 165 | POPER | L | | 166 | PERIV | L |
| 167 | FISCYEARPER | L | | 168 | BUDAT | L |
| 169 | BLDAT | L | | 170 | BLART | L |
| 171 | BUZEI | L | | 172 | ZUONR | M |
| 173 | BSCHL | L | | 174 | BSTAT | L |
| 175 | LINETYPE | M | | 176 | KTOSL | M |
| 177 | SLALITTYPE | V | | 178 | XSPLITMOD | H |
| 179 | USNAM | L | | 180 | TIMESTAMP | L |
| 181 | EPRCTR | M | | 182 | RHOART | V |
| 183 | GLACCOUNT_TYPE | L | | 184 | KTOPL | L |
| 185 | LOKKT | V | | 186 | KTOP2 | V |
| 187 | REBZG | M | | 188 | REBZJ | M |
| 189 | REBZZ | M | | 190 | REBZT | V |
| 191 | RBEST | H | | 192 | EBELN_LOGSYS | H |
| 193 | EBELN | M | | 194 | EBELP | M |
| 195 | ZEKKN | H | | 196 | SGTXT | L |
| 197 | KDAUF | M | | 198 | KDPOS | M |
| 199 | MATNR | M | | 200 | WERKS | M |
| 201 | LIFNR | L | | 202 | KUNNR | L |
| 203 | FBUDA | M | | 204 | PEROP_BEG | M |
| 205 | PEROP_END | M | | 206 | COCO_NUM | V |
| 207 | WWERT | V | | 208 | PRCTR_DRVTN_SOURCE_TYPE | M |
| 209 | KOART | L | | 210 | UMSKZ | H |
| 211 | TAX_COUNTRY | L | | 212 | MWSKZ | M |
| 213 | HBKID | V | | 214 | HKTID | V |
| 215 | VALUT | M | | 216 | XOPVW | M |
| 217 | AUGDT | M | | 218 | AUGBL | M |
| 219 | AUGGJ | M | | 220 | AFABE | H |
| 221 | ANLN1 | H | | 222 | ANLN2 | H |
| 223 | BZDAT | H | | 224 | ANBWA | H |
| 225 | MOVCAT | H | | 226 | DEPR_PERIOD | H |
| 227 | ANLGR | V | | 228 | ANLGR2 | V |
| 229 | SETTLEMENT_RULE | V | | 230 | ANLKL | H |
| 231 | KTOGR | H | | 232 | PANL1 | V |
| 233 | PANL2 | V | | 234 | UBZDT_PN | V |
| 235 | XVABG_PN | V | | 236 | PROZS_PN | V |
| 237 | XMANPROPVAL_PN | V | | 238 | KALNR | H |
| 239 | VPRSV | H | | 240 | MLAST | H |
| 241 | KZBWS | V | | 242 | XOBEW | V |
| 243 | SOBKZ | H | | 244 | VTSTAMP | V |
| 245 | MAT_KDAUF | V | | 246 | MAT_KDPOS | V |
| 247 | MAT_PSPNR | V | | 248 | MAT_PS_POSID | V |
| 249 | MAT_LIFNR | V | | 250 | BWTAR | H |
| 251 | BWKEY | H | | 252 | HPEINH | H |
| 253 | KPEINH | H | | 254 | OPEINH | V |
| 255 | VPEINH | V | | 256 | MLPTYP | H |
| 257 | MLCATEG | H | | 258 | QSBVALT | H |
| 259 | QSPROCESS | H | | 260 | PERART | H |
| 261 | MLPOSNR | H | | 262 | INV_MOV_CATEG | H |
| 263 | BUKRS_SENDER | V | | 264 | RACCT_SENDER | V |
| 265 | ACCAS_SENDER | V | | 266 | ACCASTY_SENDER | V |
| 267 | OBJNR | H | | 268 | HRKFT | H |
| 269 | HKGRP | H | | 270 | PAROB1 | H |
| 271 | PAROBSRC | H | | 272 | USPOB | H |
| 273 | CO_BELKZ | H | | 274 | CO_BEKNZ | H |
| 275 | BELTP | H | | 276 | MUVFLG | V |
| 277 | GKONT | M | | 278 | GKOAR | M |
| 279 | ERLKZ | V | | 280 | PERNR | H |
| 281 | PAOBJNR | H | | 282 | XPAOBJNR_CO_REL | H |
| 283 | SCOPE | H | | 284 | LOGSYSO | H |
| 285 | PBUKRS | L | | 286 | PSCOPE | H |
| 287 | LOGSYSP | H | | 288 | BWSTRAT | H |
| 289 | OBJNR_HK | H | | 290 | AUFNR_ORG | H |
| 291 | UKOSTL | H | | 292 | ULSTAR | H |
| 293 | UPRZNR | H | | 294 | UPRCTR | H |
| 295 | UMATNR | H | | 296 | VARC_UACCT | H |
| 297 | ACCAS | H | | 298 | ACCASTY | H |
| 299 | LSTAR | H | | 300 | AUFNR | H |
| 301 | AUTYP | H | | 302 | PS_PSP_PNR | H |
| 303 | PS_POSID | H | | 304 | PS_PRJ_PNR | H |
| 305 | PS_PSPID | H | | 306 | NPLNR | V |
| 307 | NPLNR_VORGN | V | | 308 | PRZNR | H |
| 309 | KSTRG | H | | 310 | BEMOT | H |
| 311 | RSRCE | V | | 312 | QMNUM | V |
| 313 | SERVICE_DOC_TYPE | V | | 314 | SERVICE_DOC_ID | V |
| 315 | SERVICE_DOC_ITEM_ID | V | | 316 | SERVICE_CONTRACT_TYPE | V |
| 317 | SERVICE_CONTRACT_ID | V | | 318 | SERVICE_CONTRACT_ITEM_ID | V |
| 319 | SOLUTION_ORDER_ID | V | | 320 | SOLUTION_ORDER_ITEM_ID | V |
| 321 | ERKRS | H | | 322 | PACCAS | H |
| 323 | PACCASTY | H | | 324 | PLSTAR | H |
| 325 | PAUFNR | H | | 326 | PAUTYP | H |
| 327 | PPS_PSP_PNR | H | | 328 | PPS_POSID | H |
| 329 | PPS_PRJ_PNR | V | | 330 | PPS_PSPID | V |
| 331 | PKDAUF | H | | 332 | PKDPOS | H |
| 333 | PPAOBJNR | H | | 334 | PNPLNR | V |
| 335 | PNPLNR_VORGN | V | | 336 | PPRZNR | V |
| 337 | PKSTRG | V | | 338 | PSERVICE_DOC_TYPE | V |
| 339 | PSERVICE_DOC_ID | V | | 340 | CO_ACCASTY_N1 | H |
| 341 | CO_ACCASTY_N2 | H | | 342 | CO_ACCASTY_N3 | H |
| 343 | CO_ZLENR | H | | 344 | CO_BELNR | H |
| 345 | CO_BUZEI | H | | 346 | CO_BUZEI1 | V |
| 347 | CO_BUZEI2 | V | | 348 | CO_BUZEI5 | V |
| 349 | CO_BUZEI6 | V | | 350 | CO_BUZEI7 | V |
| 351 | CO_REFBZ | V | | 352 | CO_REFBZ1 | V |
| 353 | CO_REFBZ2 | V | | 354 | CO_REFBZ5 | V |
| 355 | CO_REFBZ6 | V | | 356 | CO_REFBZ7 | V |
| 357 | OVERTIMECAT | V | | 358 | WORK_ITEM_ID | V |
| 359 | ARBID | V | | 360 | VORNR | V |
| 361 | AUFPS | V | | 362 | UVORN | V |
| 363 | EQUNR | V | | 364 | TPLNR | V |
| 365 | ISTRU | V | | 366 | ILART | V |
| 367 | PLKNZ | V | | 368 | ARTPR | V |
| 369 | PRIOK | V | | 370 | MAUFNR | V |
| 371 | MATKL_MM | M | | 372 | PAUFPS | V |
| 373 | PLANNED_PARTS_WORK | V | | 374 | FKART | M |
| 375 | VKORG | M | | 376 | VTWEG | M |
| 377 | SPART | M | | 378 | MATNR_COPA | H |
| 379 | MATKL | H | | 380 | KDGRP | H |
| 381 | LAND1 | L | | 382 | BRSCH | H |
| 383 | BZIRK | H | | 384 | KUNRE | H |
| 385 | KUNWE | H | | 386 | KONZS | H |
| 387 | ACDOC_COPA_EEW_DUMMY_PA | V | | 388 | VKGRP_PA | H |
| 389 | MAABC_PA | V | | 390 | BONUS_PA | V |
| 391 | VKBUR_PA | H | | 392 | EFORM_PA | V |
| 393 | GEBIE_PA | V | | 394 | KMVKBU_PA | V |
| 395 | KMVKGR_PA | V | | 396 | KMBRND_PA | H |
| 397 | CRMELEM_PA | V | | 398 | KMKDGR_PA | H |
| 399 | KMLAND_PA | H | | 400 | KMMAKL_PA | H |
| 401 | KMVTNR_PA | V | | 402 | WWTES_PA | V |
| 403 | KMHI01_PA | V | | 404 | KMHI02_PA | V |
| 405 | KMHI03_PA | V | | 406 | CRMCSTY_PA | V |
| 407 | KMATYP_PA | V | | 408 | KMDEST_PA | V |
| 409 | KMFLTN_PA | V | | 410 | KMFLTY_PA | V |
| 411 | KMIATA_PA | V | | 412 | KMLEGS_PA | V |
| 413 | KMOPDY_PA | V | | 414 | KMORIG_PA | V |
| 415 | KMROUT_PA | V | | 416 | KMSTGE_PA | H |
| 417 | KMWNHG_PA | V | | 418 | KMZONE_PA | V |
| 419 | PAPH1_PA | H | | 420 | PRODH_PA | H |
| 421 | WWACT_PA | V | | 422 | WWERM_PA | V |
| 423 | PAPH2_PA | H | | 424 | PAPH3_PA | H |
| 425 | COLLE_PA | V | | 426 | CRMFIGR_PA | V |
| 427 | KMCATG_PA | V | | 428 | KMNIEL_PA | V |
| 429 | SAISJ_PA | V | | 430 | SAISO_PA | V |
| 431 | ARTNRG_PA | V | | 432 | MATNR_PA | V |
| 433 | PARTNER_PA | H | | 434 | REGIO_PA | H |
| 435 | WW001_PA | V | | 436 | WW100_PA | V |
| 437 | WW101_PA | V | | 438 | WWBU1_PA | V |
| 439 | RE_BUKRS | V | | 440 | RE_ACCOUNT | V |
| 441 | FIKRS | V | | 442 | FIPEX | V |
| 443 | FISTL | V | | 444 | MEASURE | V |
| 445 | RFUND | V | | 446 | RGRANT_NBR | V |
| 447 | RBUDGET_PD | V | | 448 | SFUND | V |
| 449 | SGRANT_NBR | V | | 450 | SBUDGET_PD | V |
| 451 | BDGT_ACCOUNT | V | | 452 | BDGT_ACCOUNT_COCODE | V |
| 453 | BDGT_CNSMPN_DATE | V | | 454 | BDGT_CNSMPN_PERIOD | V |
| 455 | BDGT_CNSMPN_YEAR | V | | 456 | BDGT_RELEVANT | V |
| 457 | BDGT_CNSMPN_TYPE | V | | 458 | BDGT_CNSMPN_AMOUNT_TYPE | V |
| 459 | RSPONSORED_PROG | V | | 460 | RSPONSORED_CLASS | V |
| 461 | RBDGT_VLDTY_NBR | V | | 462 | KBLNR | V |
| 463 | KBLPOS | V | | 464 | VNAME | V |
| 465 | EGRUP | V | | 466 | RECID | V |
| 467 | VPTNR | V | | 468 | BTYPE | V |
| 469 | ETYPE | V | | 470 | PRODPER | V |
| 471 | BILLM | V | | 472 | POM | V |
| 473 | CBRUNID | V | | 474 | PVNAME | V |
| 475 | PEGRUP | V | | 476 | S_RECIND | V |
| 477 | CBRACCT | V | | 478 | CBOBJNR | V |
| 479 | SWENR | V | | 480 | SGENR | V |
| 481 | SGRNR | V | | 482 | SMENR | V |
| 483 | RECNNR | V | | 484 | SNKSL | V |
| 485 | SEMPSL | V | | 486 | DABRZ | V |
| 487 | PSWENR | V | | 488 | PSGENR | V |
| 489 | PSGRNR | V | | 490 | PSMENR | V |
| 491 | PRECNNR | V | | 492 | PSNKSL | V |
| 493 | PSEMPSL | V | | 494 | PDABRZ | V |
| 495 | ZMATNR | V | | 496 | ACROBJTYPE | V |
| 497 | ACRLOGSYS | V | | 498 | ACROBJ_ID | V |
| 499 | ACRSOBJ_ID | V | | 500 | ACRITMTYPE | V |
| 501 | ACRVALDAT | V | | 502 | VALOBJTYPE | V |
| 503 | VALOBJ_ID | V | | 504 | VALSOBJ_ID | V |
| 505 | NETDT | M | | 506 | RISK_CLASS | V |
| 507 | FUP_ACTION | V | | 508 | SDM_VERSION | M |
| 509 | MIG_SOURCE | V | | 510 | MIG_DOCLN | V |
| 511 | _DATAAGING | V | | 512 | CLOSINGSTEP | M |
| 513 | RFCCUR | M | | 514 | FCSL | M |
| 515 | RBUNIT | M | | 516 | RBUPTR | M |
| 517 | RCOMP | M | | 518 | RITCLG | M |
| 519 | RITEM | M | | 520 | SITYP | V |
| 521 | SUBIT | V | | 522 | .INCLU-_PN | X |
| 523 | ANLN2_PN | V | | 524 | BWASL_PN | V |
| 525 | BZDAT_PN | V | | 526 | ANBTR_PN | V |
| 527 | VTKEY | V | | 528 | VTPOS | V |
| 529 | RA_CONTRACT_ID | M | | 530 | RA_POB_ID | M |
| 531 | PSERVICE_DOC_ITEM_ID | V | | 532 | .INCLU-_PA | X |
| 533 | .INCLU--AP | X | | 534 | WW0RE_PA | H |
| 535 | WW0SB_PA | H | | 536 | DUMMY_MRKT_SGMNT_EEW_PS | X |
| 537 | JVACTIVITY | V | | 538 | ACRREFOBJ_ID | V |

**Field count by tier (cumulative):**

| Tier | New Fields | Cumulative Total |
|------|-----------|-----------------|
| Light | ~55 | ~55 |
| Medium | ~75 | ~130 |
| High | ~120 | ~250 |
| Very High | ~154 | ~404 |
| Excluded | 4 | — |

---

*End of Specification*
