"""Build indexed company master in Polars (same logic as master_data.build_companies)."""

from __future__ import annotations

import polars as pl

from acdoca_generator.config.countries import get_country
from acdoca_generator.config.operating_models import pick_role_for_country
from acdoca_generator.generators.amounts import fx_multiplier


def build_companies_indexed_polars(
    country_isos: list[str],
    industry_key: str,
    seed: int,
    group_currency: str,
) -> pl.DataFrame:
    rows: list[dict[str, str]] = []
    per_iso_count: dict[str, int] = {}
    for iso in sorted(country_isos):
        c = get_country(iso)
        role = pick_role_for_country(iso, industry_key)
        n = per_iso_count.get(iso, 0)
        per_iso_count[iso] = n + 1
        base = c.sample_bukrs
        code_int = base + n
        rbukrs = str(code_int).zfill(4)
        prctr = f"{rbukrs}{role.code}01"
        rcntr = f"{rbukrs}GADM01"
        rows.append(
            {
                "RBUKRS": rbukrs,
                "LAND1": iso,
                "RHCUR": c.currency,
                "RWCUR": c.currency,
                "KOKRS": rbukrs,
                "PRCTR": prctr,
                "RCNTR": rcntr,
                "SEGMENT": "SEG1",
                "ROLE_CODE": role.code,
            }
        )
    df = pl.DataFrame(rows).sort("RBUKRS")
    df = df.with_row_index("ci", offset=0)
    fx = [
        float(fx_multiplier(r["RHCUR"], group_currency, seed, 1))
        for r in df.iter_rows(named=True)
    ]
    return df.with_columns(pl.Series("FX_KSL", fx))
