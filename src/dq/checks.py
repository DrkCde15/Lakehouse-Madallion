"""Data quality checks para as camadas do lakehouse (Bronze/Silver/Gold).

Severidades:
- ERROR: falha a task do Airflow — bloqueia a propagacao para a proxima camada
- WARN : apenas registra no log (nao bloqueia o pipeline)

Execucao:
    python -m src.dq.checks bronze|silver|gold
"""

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.processing.Silver import DEDUP_KEYS
from src.session import get_spark

logger = logging.getLogger(__name__)

TABLES = ("payments", "customers", "orders", "order_items", "products", "reviews")
GOLD_TABLES = ("vendas_por_categoria", "pedidos_por_status", "avaliacao_produto", "resumo_clientes")

# Status validos apos o lower(trim()) da Silver
ALLOWED_STATUSES = {"delivered", "shipped", "cancelled", "processing"}

# Sentinelas de nulo da Silver — nao contam como orfaos na referencialidade
SENTINELS = {"desconhecido"}

ERROR = "ERROR"
WARN = "WARN"


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str
    passed: bool
    detail: str = ""


@dataclass
class DQReport:
    layer: str
    results: list = field(default_factory=list)

    def add(self, table: str, check: str, severity: str, passed: bool, detail: str = "") -> None:
        self.results.append(CheckResult(table, check, severity, passed, detail))

    @property
    def errors(self) -> list:
        return [r for r in self.results if r.severity == ERROR and not r.passed]

    @property
    def warnings(self) -> list:
        return [r for r in self.results if r.severity == WARN and not r.passed]

    @property
    def ok(self) -> bool:
        return not self.errors

    def log_summary(self) -> None:
        for r in self.results:
            if r.passed:
                continue
            log = logger.error if r.severity == ERROR else logger.warning
            log("DQ FAIL [%s] %s :: %s — %s", r.severity, r.table, r.check, r.detail)
        logger.info(
            "DQ layer=%s checks=%d passed=%d errors=%d warnings=%d",
            self.layer, len(self.results), len(self.results) - len(self.errors) - len(self.warnings),
            len(self.errors), len(self.warnings),
        )


# ---------------------------------------------------------------
# Primitivas de check
# ---------------------------------------------------------------

def _load(spark, layer: str, table: str, report: DQReport) -> DataFrame | None:
    name = f"{layer}.{table}"
    try:
        df = spark.table(name)
    except Exception:
        report.add(name, "table_exists", ERROR, False, "tabela ausente")
        return None
    report.add(name, "table_exists", ERROR, True)
    return df


def _not_empty(df: DataFrame, name: str, report: DQReport) -> None:
    n = df.count()
    report.add(name, "not_empty", ERROR, n > 0, f"count={n}")


def _columns_exist(df: DataFrame, name: str, cols: list, report: DQReport) -> None:
    missing = [c for c in cols if c not in df.columns]
    report.add(name, f"columns_exist{tuple(cols)}", ERROR, not missing, f"ausentes={missing}")


def _unique(df: DataFrame, name: str, cols: list, report: DQReport) -> None:
    total = df.count()
    distinct = df.select(*cols).distinct().count()
    report.add(
        name, f"unique({'+'.join(cols)})", ERROR, total == distinct,
        f"{total} linhas vs {distinct} chaves distintas",
    )


def _not_null(df: DataFrame, name: str, cols: list, report: DQReport, severity: str = ERROR) -> None:
    for c in cols:
        n = df.filter(F.col(c).isNull()).count()
        report.add(name, f"not_null({c})", severity, n == 0, f"{n} nulos")


def _range(df: DataFrame, name: str, col: str, lo, hi, report: DQReport, severity: str = ERROR) -> None:
    n = df.filter(F.col(col).isNotNull() & ((F.col(col) < lo) | (F.col(col) > hi))).count()
    report.add(name, f"range({col}, {lo}..{hi})", severity, n == 0, f"{n} valores fora da faixa")


def _non_negative(df: DataFrame, name: str, col: str, report: DQReport, severity: str = WARN) -> None:
    n = df.filter(F.col(col).isNotNull() & (F.col(col) < 0)).count()
    report.add(name, f"non_negative({col})", severity, n == 0, f"{n} negativos")


def _in_set(df: DataFrame, name: str, col: str, allowed: set, report: DQReport, severity: str = ERROR) -> None:
    n = df.filter(F.col(col).isNotNull() & ~F.col(col).isin(list(allowed))).count()
    report.add(name, f"in_set({col})", severity, n == 0, f"{n} valores fora do conjunto")


def _orphan(
    df: DataFrame, name: str, col: str,
    parent: DataFrame, parent_col: str,
    report: DQReport, exclude: set = frozenset(), severity: str = ERROR,
) -> None:
    child = (
        df.filter(F.col(col).isNotNull() & ~F.col(col).isin(list(exclude)))
        .select(col).distinct()
    )
    parent_keys = parent.select(parent_col).distinct()
    n = (
        child.alias("c")
        .join(parent_keys.alias("p"), F.col(f"c.{col}") == F.col(f"p.{parent_col}"), "left_anti")
        .count()
    )
    report.add(
        name, f"referential({col} -> {parent_col})", severity, n == 0,
        f"{n} orfaos (sentinelas {sorted(exclude)} ignoradas)",
    )


# ---------------------------------------------------------------
# Regras por camada
# ---------------------------------------------------------------

def run_bronze(spark) -> DQReport:
    report = DQReport("bronze")
    for table in TABLES:
        df = _load(spark, "bronze", table, report)
        if df is None:
            continue
        name = f"bronze.{table}"
        _not_empty(df, name, report)
        _columns_exist(df, name, ["_ingested_at"], report)
        _not_null(df, name, ["_ingested_at"], report)
    return report


def run_silver(spark) -> DQReport:
    report = DQReport("silver")
    frames: dict[str, DataFrame] = {}
    for table in TABLES:
        df = _load(spark, "silver", table, report)
        if df is None:
            continue
        frames[table] = df
        name = f"silver.{table}"
        _not_empty(df, name, report)
        _not_null(df, name, DEDUP_KEYS[table], report)
        _unique(df, name, DEDUP_KEYS[table], report)

    if "customers" in frames:
        name = "silver.customers"
        _not_null(frames["customers"], name, ["signup_date"], report)

    if "orders" in frames:
        name = "silver.orders"
        df = frames["orders"]
        _not_null(df, name, ["order_date", "total_amount"], report)
        _in_set(df, name, "status", ALLOWED_STATUSES, report, ERROR)
        _non_negative(df, name, "total_amount", report, WARN)
        if "customers" in frames:
            _orphan(df, name, "customer_id", frames["customers"], "customer_id",
                    report, exclude=SENTINELS, severity=WARN)

    if "payments" in frames:
        name = "silver.payments"
        df = frames["payments"]
        _not_null(df, name, ["payment_value"], report)
        _non_negative(df, name, "payment_value", report, WARN)
        if "orders" in frames:
            _orphan(df, name, "order_id", frames["orders"], "order_id", report)

    if "order_items" in frames:
        name = "silver.order_items"
        df = frames["order_items"]
        _not_null(df, name, ["quantity", "unit_price", "product_id"], report)
        _non_negative(df, name, "quantity", report, WARN)
        if "orders" in frames:
            _orphan(df, name, "order_id", frames["orders"], "order_id", report)
        if "products" in frames:
            _orphan(df, name, "product_id", frames["products"], "product_id", report)

    if "reviews" in frames:
        name = "silver.reviews"
        df = frames["reviews"]
        _not_null(df, name, ["rating", "review_date"], report)
        _range(df, name, "rating", 1, 5, report, ERROR)
        if "orders" in frames:
            _orphan(df, name, "order_id", frames["orders"], "order_id", report)
        if "customers" in frames:
            _orphan(df, name, "customer_id", frames["customers"], "customer_id",
                    report, exclude=SENTINELS, severity=WARN)

    return report


GOLD_METRICS = {
    "vendas_por_categoria": ["category", "receita_total", "itens_vendidos", "total_pedidos"],
    "pedidos_por_status": ["status", "total_pedidos", "receita_total", "ticket_medio"],
    "avaliacao_produto": ["product_id", "avaliacao_media", "total_avaliacoes"],
    "resumo_clientes": ["customer_id", "total_pedidos", "total_gasto", "ticket_medio"],
}


def run_gold(spark) -> DQReport:
    report = DQReport("gold")
    frames: dict[str, DataFrame] = {}
    for table in GOLD_TABLES:
        df = _load(spark, "gold", table, report)
        if df is None:
            continue
        frames[table] = df
        name = f"gold.{table}"
        _not_empty(df, name, report)
        _not_null(df, name, GOLD_METRICS[table], report)

    if "avaliacao_produto" in frames:
        _range(frames["avaliacao_produto"], "gold.avaliacao_produto",
               "avaliacao_media", 1, 5, report, ERROR)

    if "vendas_por_categoria" in frames:
        _non_negative(frames["vendas_por_categoria"], "gold.vendas_por_categoria",
                      "receita_total", report, WARN)

    if "pedidos_por_status" in frames:
        _non_negative(frames["pedidos_por_status"], "gold.pedidos_por_status",
                      "receita_total", report, WARN)

    if "resumo_clientes" in frames:
        _non_negative(frames["resumo_clientes"], "gold.resumo_clientes",
                      "total_gasto", report, WARN)

    return report


LAYERS = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Data quality checks do lakehouse")
    parser.add_argument("layer", choices=tuple(LAYERS), help="Camada a validar")
    args = parser.parse_args(argv)

    report = LAYERS[args.layer](get_spark(f"dq-{args.layer}"))
    report.log_summary()
    if not report.ok:
        logger.error("DQ bloqueou a camada %s: %d erro(s)", args.layer, len(report.errors))
        sys.exit(1)
    logger.info("DQ camada %s OK", args.layer)


if __name__ == "__main__":
    main()
