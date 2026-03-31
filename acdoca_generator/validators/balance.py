"""Post-generation validation (SPEC §8)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class ValidationResult:
    name: str
    passed: bool
    failure_severity: str  # FAIL or WARN when passed is False
    detail: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)

    @property
    def display_severity(self) -> str:
        return "OK" if self.passed else self.failure_severity


def _doc_balance(df: DataFrame) -> ValidationResult:
    key = ["RCLNT", "RLDNR", "RBUKRS", "GJAHR", "BELNR"]
    sums = df.groupBy(*key).agg(F.sum("WSL").alias("net"))
    bad = sums.filter(F.col("net").cast("double") != 0.0).count()
    tot = sums.count()
    ok = bad == 0
    return ValidationResult(
        "DOC_BALANCE",
        ok,
        "FAIL",
        f"Unbalanced documents: {bad} of {tot}",
        {"unbalanced_docs": bad, "doc_groups": tot},
    )


def _pk_unique(df: DataFrame) -> ValidationResult:
    key = ["RCLNT", "RLDNR", "RBUKRS", "GJAHR", "BELNR", "DOCLN"]
    c = df.count()
    d = df.select(*key).distinct().count()
    ok = c == d
    return ValidationResult(
        "PK_UNIQUE",
        ok,
        "FAIL",
        f"rows={c} distinct_pk={d}",
        {"rows": c, "distinct_pk": d},
    )


def _drcrk_sign(df: DataFrame) -> ValidationResult:
    bad = df.filter(
        ((F.col("DRCRK") == "S") & (F.col("WSL") <= 0))
        | ((F.col("DRCRK") == "H") & (F.col("WSL") >= 0))
    ).limit(1).count()
    ok = bad == 0
    return ValidationResult(
        "DRCRK_SIGN",
        ok,
        "FAIL",
        "WSL sign must be positive for S and negative for H" if not ok else "ok",
        {"violations": bad},
    )


def _ic_pair(df: DataFrame) -> ValidationResult:
    ic = df.filter(F.col("RASSC") != "")
    if ic.limit(1).count() == 0:
        return ValidationResult("IC_PAIR", True, "WARN", "No IC lines", {})
    paired = (
        ic.groupBy("AWREF")
        .agg(F.countDistinct("RBUKRS").alias("cc"))
        .filter(F.col("cc") >= 2)
        .count()
    )
    awrefs = ic.select("AWREF").distinct().count()
    ok = paired == awrefs if awrefs > 0 else True
    return ValidationResult(
        "IC_PAIR",
        ok,
        "WARN",
        f"AWREF with multi-CC: {paired} / distinct AWREF {awrefs}",
        {"paired_awref": paired, "distinct_awref": awrefs},
    )


def _ic_amount(df: DataFrame) -> ValidationResult:
    ic = df.filter((F.col("RASSC") != "") & (F.col("AWREF") != ""))
    if ic.limit(1).count() == 0:
        return ValidationResult("IC_AMOUNT", True, "WARN", "No IC lines", {})
    g = ic.groupBy("AWREF", "RWCUR").agg(F.sum("WSL").alias("net"))
    bad = g.filter(F.abs(F.col("net")) > F.lit(0.01)).limit(5).count()
    ok = bad == 0
    return ValidationResult(
        "IC_AMOUNT",
        ok,
        "WARN",
        f"Non-zero net WSL buckets: {bad}",
        {"bad_buckets": bad},
    )


def _period_valid(df: DataFrame) -> ValidationResult:
    bad = df.filter(
        (F.col("POPER").cast("int") < 1) | (F.col("POPER").cast("int") > 16)
    ).limit(1).count()
    ok = bad == 0
    return ValidationResult("PERIOD_VALID", ok, "FAIL", f"bad_popers={bad}", {})


def _currency_set(df: DataFrame) -> ValidationResult:
    bad = df.filter((F.col("RHCUR").isNull()) | (F.col("RHCUR") == "")).limit(1).count()
    ok = bad == 0
    return ValidationResult("CURRENCY_SET", ok, "WARN", f"missing_rhcur_rows={bad}", {})


def _prctr_assigned(df: DataFrame) -> ValidationResult:
    pnl = df.filter(F.col("GLACCOUNT_TYPE") == "P")
    bad = pnl.filter((F.col("PRCTR").isNull()) | (F.col("PRCTR") == "")).limit(1).count()
    ok = bad == 0
    return ValidationResult("PRCTR_ASSIGNED", ok, "WARN", f"missing_prctr_pnl={bad}", {})


def run_validations(df: DataFrame) -> List[ValidationResult]:
    return [
        _doc_balance(df),
        _pk_unique(df),
        _drcrk_sign(df),
        _period_valid(df),
        _ic_pair(df),
        _ic_amount(df),
        _currency_set(df),
        _prctr_assigned(df),
    ]


def blocking_failures(results: List[ValidationResult]) -> List[ValidationResult]:
    return [r for r in results if not r.passed and r.failure_severity == "FAIL"]
