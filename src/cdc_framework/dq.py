"""Metadata-driven data quality.

Rules are turned into Spark SQL predicates (pure Python, unit-testable), then
applied to a DataFrame in Fabric notebooks. pyspark is imported lazily so this
module can be tested without Spark.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from cdc_framework.config import DqRule

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame


def _ident(column: str) -> str:
    return "`" + column.replace("`", "``") + "`"


def _literal(value: Any) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int | float):
        return repr(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def rule_to_predicate(rule: DqRule) -> str | None:
    """Spark SQL expression that is TRUE when a row PASSES the rule.

    Returns None for set-level rules (unique) that need a window/aggregate.
    NULLs pass every rule except not_null, so rules compose cleanly.
    """
    col = _ident(rule.column)
    if rule.rule_type == "not_null":
        return f"{col} IS NOT NULL"
    if rule.rule_type == "range":
        parts = []
        if rule.min is not None:
            parts.append(f"{col} >= {_literal(rule.min)}")
        if rule.max is not None:
            parts.append(f"{col} <= {_literal(rule.max)}")
        return f"({col} IS NULL OR ({' AND '.join(parts)}))"
    if rule.rule_type == "regex":
        return f"({col} IS NULL OR {col} RLIKE {_literal(rule.pattern)})"
    if rule.rule_type == "accepted_values":
        values = ", ".join(_literal(v) for v in rule.values or [])
        return f"({col} IS NULL OR {col} IN ({values}))"
    if rule.rule_type == "unique":
        return None
    raise ValueError(f"unsupported rule_type {rule.rule_type}")


def rule_name(rule: DqRule) -> str:
    return f"{rule.rule_type}:{rule.column}"


@dataclass
class DqResult:
    rule: str
    action: str
    failed_rows: int
    total_rows: int
    threshold: float

    @property
    def failed_fraction(self) -> float:
        return self.failed_rows / self.total_rows if self.total_rows else 0.0

    @property
    def breached(self) -> bool:
        return self.failed_rows > 0 and self.failed_fraction > self.threshold

    def to_dict(self) -> dict:
        return {**asdict(self), "failed_fraction": self.failed_fraction, "breached": self.breached}


class DqFailure(Exception):
    """Raised when a rule with action=fail breaches its threshold."""


def apply_dq(df: DataFrame, rules: list[DqRule]) -> tuple[DataFrame, DataFrame, list[DqResult]]:
    """Evaluate rules against df.

    Returns (valid_df, quarantine_df, results).
      - warn:       counted only
      - quarantine: failing rows are moved to quarantine_df with a _dq_failed_rules column
      - fail:       raises DqFailure if the threshold is breached
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    flagged = df
    flag_cols: list[tuple[str, DqRule]] = []
    for i, rule in enumerate(rules):
        flag = f"__dq_fail_{i}"
        predicate = rule_to_predicate(rule)
        if predicate is None:  # unique
            w = Window.partitionBy(F.col(rule.column))
            flagged = flagged.withColumn(
                flag, F.col(rule.column).isNotNull() & (F.count(F.lit(1)).over(w) > 1)
            )
        else:
            flagged = flagged.withColumn(flag, ~F.expr(predicate))
        flag_cols.append((flag, rule))

    flagged = flagged.cache()
    total = flagged.count()

    counts = (
        flagged.agg(*[F.sum(F.col(f).cast("int")).alias(f) for f, _ in flag_cols]).first().asDict()
        if flag_cols
        else {}
    )
    results = [
        DqResult(rule_name(r), r.action, int(counts.get(f) or 0), total, r.threshold) for f, r in flag_cols
    ]

    failures = [res for res in results if res.action == "fail" and res.breached]
    if failures:
        flagged.unpersist()
        raise DqFailure("; ".join(f"{r.rule} failed {r.failed_rows}/{r.total_rows}" for r in failures))

    quarantine_flags = [(f, r) for f, r in flag_cols if r.action == "quarantine"]
    failed_names = F.array_compact(
        F.array(*[F.when(F.col(f), F.lit(rule_name(r))) for f, r in quarantine_flags])
    ) if quarantine_flags else F.array().cast("array<string>")
    flagged = flagged.withColumn("_dq_failed_rules", failed_names)

    drop = [f for f, _ in flag_cols]
    valid = flagged.where(F.size("_dq_failed_rules") == 0).drop(*drop, "_dq_failed_rules")
    quarantine = flagged.where(F.size("_dq_failed_rules") > 0).drop(*drop)
    return valid, quarantine, results
