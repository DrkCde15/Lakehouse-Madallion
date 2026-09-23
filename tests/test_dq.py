"""Tests dos checks de data quality (src/dq/checks.py)."""

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.dq.checks import (  # noqa: E402
    DQReport,
    _in_set,
    _non_negative,
    _not_null,
    _orphan,
    _range,
    _unique,
    WARN,
)


def failed(report, severity=None):
    return [
        r for r in report.results
        if not r.passed and (severity is None or r.severity == severity)
    ]


def test_unique_passes_on_distinct_keys(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], "id int")
    report = DQReport("silver")
    _unique(df, "silver.t", ["id"], report)
    assert report.ok
    assert report.results[0].passed


def test_unique_fails_on_duplicates(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], "id int")
    report = DQReport("silver")
    _unique(df, "silver.t", ["id"], report)
    assert not report.ok
    assert failed(report, "ERROR")
    assert "3 linhas vs 2 chaves distintas" in report.results[0].detail


def test_not_null_detects_nulls(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], "id int, name string")
    report = DQReport("silver")
    _not_null(df, "silver.t", ["name"], report)
    assert not report.ok
    assert failed(report, "ERROR")


def test_range_error_on_out_of_bounds(spark):
    df = spark.createDataFrame([(1,), (6,), (0,)], "rating int")
    report = DQReport("silver")
    _range(df, "silver.t", "rating", 1, 5, report)
    assert len(failed(report, "ERROR")) == 1


def test_range_ignores_nulls(spark):
    df = spark.createDataFrame([(1,), (None,)], "rating int")
    report = DQReport("silver")
    _range(df, "silver.t", "rating", 1, 5, report)
    assert report.ok


def test_non_negative_is_warn_by_default(spark):
    df = spark.createDataFrame([(10.0,), (-100.0,)], "amount double")
    report = DQReport("silver")
    _non_negative(df, "silver.t", "amount", report)
    assert report.ok  # WARN nao bloqueia
    assert failed(report, WARN)
    assert len(report.warnings) == 1


def test_in_set_detects_invalid_values(spark):
    df = spark.createDataFrame([("delivered",), ("unknown",)], "status string")
    report = DQReport("silver")
    _in_set(df, "silver.t", "status", {"delivered", "shipped"}, report)
    assert not report.ok


def test_orphan_excludes_sentinels(spark):
    child = spark.createDataFrame([("1",), ("999",), ("desconhecido",)], "customer_id string")
    parent = spark.createDataFrame([("1",), ("2",)], "customer_id string")
    report = DQReport("silver")
    _orphan(child, "silver.t", "customer_id", parent, "customer_id",
            report, exclude={"desconhecido"})
    # "999" e orfao; "desconhecido" ignorado
    assert not report.ok
    assert "1 orfaos" in report.results[0].detail


def test_orphan_error_when_real_orphan(spark):
    child = spark.createDataFrame([("1",), ("2",)], "order_id string")
    parent = spark.createDataFrame([("1",)], "order_id string")
    report = DQReport("silver")
    _orphan(child, "silver.t", "order_id", parent, "order_id", report)
    assert not report.ok
    assert failed(report, "ERROR")


def test_report_ok_blocked_by_errors_only(spark):
    df = spark.createDataFrame([(10.0,), (-1.0,)], "amount double")
    report = DQReport("gold")
    _non_negative(df, "gold.t", "amount", report)  # WARN
    assert report.ok

    report.add("gold.t", "not_empty", "ERROR", False, "count=0")
    assert not report.ok
    assert len(report.errors) == 1
    assert len(report.warnings) == 1
