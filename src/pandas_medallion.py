"""Versão pandas do pipeline Medallion para uso no notebook.

Espelha a semântica dos jobs PySpark (`src/ingestion/Bronze.py`,
`src/processing/Silver.py`, `src/serving/Gold.py` e `src/dq/checks.py`)
lendo direto os CSVs de ``data/`` — sem Spark, sem Java, sem MinIO/Delta.

Diferenças intencionais (documentadas):
- ``_ingested_at`` já nasce no fuso America/Sao_Paulo (o job Spark grava em
  UTC e converte na Silver; aqui não há conversão tardia).
- Strings vazias (``""``) são tratadas como ausentes, além de nulos reais.
  No Spark os CSVs chegam como string e ``""`` não é ``NULL``, então um
  ``""`` escapava dos defaults; aqui ele recebe o default (comportamento
  desejado — verificado nos CSVs reais: ``email``, ``state``,
  ``customer_id``, ``product_name`` e ``comment`` usam ``""``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SP_TZ = ZoneInfo("America/Sao_Paulo")

TABLE_NAMES = ("payments", "customers", "orders", "order_items", "products", "reviews")
GOLD_TABLES = ("vendas_por_categoria", "pedidos_por_status", "avaliacao_produto", "resumo_clientes")

DEDUP_KEYS = {
    "payments": ["payment_id"],
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "item_id"],
    "products": ["product_id"],
    "reviews": ["review_id"],
}

ALLOWED_STATUSES = {"delivered", "shipped", "cancelled", "processing"}
SENTINELS = {"desconhecido"}

ERROR = "ERROR"
WARN = "WARN"


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

def _is_missing(s: pd.Series) -> pd.Series:
    """Nulo real ou string vazia / só-espaços."""
    return s.isna() | (s.astype(str).str.strip() == "")


def _strip_prefix(s: pd.Series, pattern: str) -> pd.Series:
    return s.astype(str).str.replace(pattern, "", regex=True)


def _norm_lower(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()


def _to_number(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _to_int(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def _fill_missing(s: pd.Series, default: str) -> pd.Series:
    out = s.copy()
    out[_is_missing(out)] = default
    return out


def _now_sp() -> datetime:
    return datetime.now(SP_TZ)


# ---------------------------------------------------------------
# Bronze — CSVs -> DataFrames (tudo string + _ingested_at)
# ---------------------------------------------------------------

def load_bronze(data_dir: Path | str = DATA_DIR) -> dict[str, pd.DataFrame]:
    """Lê os 6 CSVs como string (fiel ao read_csv do Spark) + _ingested_at."""
    data_dir = Path(data_dir)
    tables: dict[str, pd.DataFrame] = {}
    for name in TABLE_NAMES:
        path = data_dir / f"{name}.csv"
        if not path.exists():
            raise FileNotFoundError(f"CSV não encontrado: {path}")
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")
        df["_ingested_at"] = _now_sp()
        tables[name] = df
        logger.info("bronze.%s | %d linhas | %d colunas", name, len(df), len(df.columns))
    return tables


# ---------------------------------------------------------------
# Silver — deduplicação, tipagem e normalização
# ---------------------------------------------------------------

def transform_payments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["payment_id"] = _strip_prefix(out["payment_id"], r"^PAY0*")
    out["order_id"] = _strip_prefix(out["order_id"], r"^O0*")
    out["payment_type"] = _norm_lower(out["payment_type"])
    out["payment_value"] = _to_number(out["payment_value"])
    return out


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["customer_id"] = _strip_prefix(out["customer_id"], r"^C0*")
    out["email"] = _fill_missing(out["email"], "não informado")
    out["state"] = _fill_missing(out["state"], "desconhecido")
    out["signup_date"] = _to_date(out["signup_date"])
    return out


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_id"] = _strip_prefix(out["order_id"], r"^O0*")
    cust = _fill_missing(out["customer_id"], "desconhecido")
    needs_strip = cust != "desconhecido"
    cust[needs_strip] = _strip_prefix(cust[needs_strip], r"^C0*")
    out["customer_id"] = cust
    out["order_date"] = _to_date(out["order_date"])
    out["status"] = _norm_lower(out["status"])
    out["total_amount"] = _to_number(out["total_amount"])
    return out


def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_id"] = _strip_prefix(out["order_id"], r"^O0*")
    out["item_id"] = _strip_prefix(out["item_id"], r"^O0*")
    out["product_id"] = _strip_prefix(out["product_id"], r"^P0*")
    out["quantity"] = _to_int(out["quantity"])
    out["unit_price"] = _to_number(out["unit_price"])
    return out


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["product_id"] = _strip_prefix(out["product_id"], r"^P0*")
    out["product_name"] = _fill_missing(out["product_name"], "Produto sem nome")
    out["price"] = _to_number(out["price"])
    out["stock"] = _to_int(out["stock"])
    return out


def transform_reviews(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["review_id"] = _strip_prefix(out["review_id"], r"^R0*")
    out["order_id"] = _strip_prefix(out["order_id"], r"^O0*")
    out["customer_id"] = _strip_prefix(out["customer_id"], r"^C0*")
    out["rating"] = _to_int(out["rating"])
    out["comment"] = _fill_missing(out["comment"], "sem comentário")
    out["review_date"] = _to_date(out["review_date"])
    return out


TABLE_TRANSFORMS = {
    "payments": transform_payments,
    "customers": transform_customers,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "products": transform_products,
    "reviews": transform_reviews,
}


def run_silver(bronze: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Dedup por chave natural + transform + _ingested_at no fim."""
    silver: dict[str, pd.DataFrame] = {}
    for name, fn in TABLE_TRANSFORMS.items():
        df = bronze[name].drop_duplicates(subset=DEDUP_KEYS[name])
        df = fn(df)
        cols = [c for c in df.columns if c != "_ingested_at"] + ["_ingested_at"]
        df = df[cols]
        silver[name] = df
        logger.info("silver.%s | %d linhas | %d colunas", name, len(df), len(df.columns))
    return silver


# ---------------------------------------------------------------
# Gold — agregações de negócio (mesma semântica do Spark)
# ---------------------------------------------------------------

def build_vendas_por_categoria(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged = silver["order_items"].merge(silver["products"], on="product_id", how="left")
    merged["receita"] = merged["quantity"].astype(float) * merged["unit_price"].astype(float)
    g = merged.groupby("category", dropna=False)
    out = g.agg(
        receita_total=("receita", "sum"),
        itens_vendidos=("quantity", "sum"),
        produtos_distintos=("product_id", "nunique"),
        total_pedidos=("order_id", "nunique"),
    ).reset_index().sort_values("receita_total", ascending=False)
    return out


def build_pedidos_por_status(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    g = silver["orders"].groupby("status", dropna=False)
    out = g.agg(
        total_pedidos=("order_id", "nunique"),
        receita_total=("total_amount", "sum"),
        ticket_medio=("total_amount", "mean"),
    ).reset_index().sort_values("receita_total", ascending=False)
    return out


def build_avaliacao_produto(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged = (
        silver["reviews"]
        .merge(silver["order_items"], on="order_id", how="left")
        .merge(silver["products"], on="product_id", how="left")
        # Uma avaliação do pedido conta no máximo 1x por produto (anti fan-out)
        .drop_duplicates(subset=["review_id", "product_id"])
    )
    g = merged.groupby(["product_id", "product_name", "category"], dropna=False)
    out = g.agg(
        avaliacao_media=("rating", "mean"),
        total_avaliacoes=("review_id", "nunique"),
        pior_nota=("rating", "min"),
        melhor_nota=("rating", "max"),
    ).reset_index().sort_values("avaliacao_media", ascending=False)
    return out


def build_resumo_clientes(silver: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged = silver["orders"].merge(silver["customers"], on="customer_id", how="left")
    g = merged.groupby(["customer_id", "customer_name", "state", "city"], dropna=False)
    out = g.agg(
        total_pedidos=("order_id", "nunique"),
        total_gasto=("total_amount", "sum"),
        ticket_medio=("total_amount", "mean"),
        primeiro_pedido=("order_date", "min"),
        ultimo_pedido=("order_date", "max"),
    ).reset_index().sort_values("total_gasto", ascending=False)
    return out


GOLD_BUILDERS = {
    "vendas_por_categoria": build_vendas_por_categoria,
    "pedidos_por_status": build_pedidos_por_status,
    "avaliacao_produto": build_avaliacao_produto,
    "resumo_clientes": build_resumo_clientes,
}


def run_gold(silver: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {name: fn(silver) for name, fn in GOLD_BUILDERS.items()}


# ---------------------------------------------------------------
# DQ em pandas — mesma severidade do src/dq/checks.py
# ---------------------------------------------------------------

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

    def summary(self) -> str:
        n = len(self.results)
        return (
            f"DQ layer={self.layer} checks={n} "
            f"passed={n - len(self.errors) - len(self.warnings)} "
            f"errors={len(self.errors)} warnings={len(self.warnings)}"
        )

    def failures_table(self) -> pd.DataFrame:
        rows = [
            {"severity": r.severity, "tabela": r.table, "check": r.check, "detalhe": r.detail}
            for r in self.results if not r.passed
        ]
        if not rows:
            return pd.DataFrame([{"resultado": "tudo passou ✔"}])
        return pd.DataFrame(rows)


def _check_not_empty(df: pd.DataFrame, name: str, report: DQReport) -> None:
    report.add(name, "not_empty", ERROR, len(df) > 0, f"count={len(df)}")


def _check_unique(df: pd.DataFrame, name: str, cols: list, report: DQReport) -> None:
    distinct = df.drop_duplicates(subset=cols).shape[0]
    report.add(
        name, f"unique({'+'.join(cols)})", ERROR, len(df) == distinct,
        f"{len(df)} linhas vs {distinct} chaves distintas",
    )


def _check_not_null(df: pd.DataFrame, name: str, cols: list, report: DQReport,
                    severity: str = ERROR) -> None:
    for c in cols:
        n = int(df[c].isna().sum())
        report.add(name, f"not_null({c})", severity, n == 0, f"{n} nulos")


def _check_range(df: pd.DataFrame, name: str, col: str, lo, hi, report: DQReport,
                 severity: str = ERROR) -> None:
    s = pd.to_numeric(df[col], errors="coerce")
    n = int(((s < lo) | (s > hi)).sum())
    report.add(name, f"range({col}, {lo}..{hi})", severity, n == 0, f"{n} fora da faixa")


def _check_non_negative(df: pd.DataFrame, name: str, col: str, report: DQReport,
                        severity: str = WARN) -> None:
    s = pd.to_numeric(df[col], errors="coerce")
    n = int((s < 0).sum())
    report.add(name, f"non_negative({col})", severity, n == 0, f"{n} negativos")


def _check_in_set(df: pd.DataFrame, name: str, col: str, allowed: set, report: DQReport,
                  severity: str = ERROR) -> None:
    s = df[col]
    n = int((s.notna() & ~s.isin(allowed)).sum())
    report.add(name, f"in_set({col})", severity, n == 0, f"{n} fora do conjunto")


def _check_orphan(df: pd.DataFrame, name: str, col: str, parent: pd.DataFrame, parent_col: str,
                  report: DQReport, exclude: set = frozenset(), severity: str = ERROR) -> None:
    child_keys = set(df.loc[df[col].notna() & ~df[col].isin(exclude), col].drop_duplicates())
    parent_keys = set(parent[parent_col].dropna().drop_duplicates())
    n = len(child_keys - parent_keys)
    report.add(
        name, f"referential({col} -> {parent_col})", severity, n == 0,
        f"{n} orfaos (sentinelas {sorted(exclude)} ignoradas)",
    )


GOLD_METRICS = {
    "vendas_por_categoria": ["category", "receita_total", "itens_vendidos", "total_pedidos"],
    "pedidos_por_status": ["status", "total_pedidos", "receita_total", "ticket_medio"],
    "avaliacao_produto": ["product_id", "avaliacao_media", "total_avaliacoes"],
    "resumo_clientes": ["customer_id", "total_pedidos", "total_gasto", "ticket_medio"],
}


def run_dq_bronze(bronze: dict[str, pd.DataFrame]) -> DQReport:
    report = DQReport("bronze")
    for name, df in bronze.items():
        full = f"bronze.{name}"
        report.add(full, "table_exists", ERROR, True)
        _check_not_empty(df, full, report)
        missing = [c for c in ["_ingested_at"] if c not in df.columns]
        report.add(full, "columns_exist('_ingested_at',)", ERROR, not missing, f"ausentes={missing}")
        _check_not_null(df, full, ["_ingested_at"], report)
    return report


def run_dq_silver(silver: dict[str, pd.DataFrame]) -> DQReport:
    report = DQReport("silver")
    for name, df in silver.items():
        full = f"silver.{name}"
        report.add(full, "table_exists", ERROR, True)
        _check_not_empty(df, full, report)
        _check_not_null(df, full, DEDUP_KEYS[name], report)
        _check_unique(df, full, DEDUP_KEYS[name], report)

    if "customers" in silver:
        _check_not_null(silver["customers"], "silver.customers", ["signup_date"], report)
    if "orders" in silver:
        df = silver["orders"]
        _check_not_null(df, "silver.orders", ["order_date", "total_amount"], report)
        _check_in_set(df, "silver.orders", "status", ALLOWED_STATUSES, report, ERROR)
        _check_non_negative(df, "silver.orders", "total_amount", report, WARN)
        if "customers" in silver:
            _check_orphan(df, "silver.orders", "customer_id", silver["customers"], "customer_id",
                          report, exclude=SENTINELS, severity=WARN)
    if "payments" in silver:
        df = silver["payments"]
        _check_not_null(df, "silver.payments", ["payment_value"], report)
        _check_non_negative(df, "silver.payments", "payment_value", report, WARN)
        if "orders" in silver:
            _check_orphan(df, "silver.payments", "order_id", silver["orders"], "order_id", report)
    if "order_items" in silver:
        df = silver["order_items"]
        _check_not_null(df, "silver.order_items", ["quantity", "unit_price", "product_id"], report)
        _check_non_negative(df, "silver.order_items", "quantity", report, WARN)
        if "orders" in silver:
            _check_orphan(df, "silver.order_items", "order_id", silver["orders"], "order_id", report)
        if "products" in silver:
            _check_orphan(df, "silver.order_items", "product_id", silver["products"], "product_id", report)
    if "reviews" in silver:
        df = silver["reviews"]
        _check_not_null(df, "silver.reviews", ["rating", "review_date"], report)
        _check_range(df, "silver.reviews", "rating", 1, 5, report, ERROR)
        if "orders" in silver:
            _check_orphan(df, "silver.reviews", "order_id", silver["orders"], "order_id", report)
        if "customers" in silver:
            _check_orphan(df, "silver.reviews", "customer_id", silver["customers"], "customer_id",
                          report, exclude=SENTINELS, severity=WARN)
    return report


def run_dq_gold(gold: dict[str, pd.DataFrame]) -> DQReport:
    report = DQReport("gold")
    for name, df in gold.items():
        full = f"gold.{name}"
        report.add(full, "table_exists", ERROR, True)
        _check_not_empty(df, full, report)
        _check_not_null(df, full, GOLD_METRICS[name], report)

    if "avaliacao_produto" in gold:
        _check_range(gold["avaliacao_produto"], "gold.avaliacao_produto",
                     "avaliacao_media", 1, 5, report, ERROR)
    for t, col in (("vendas_por_categoria", "receita_total"),
                   ("pedidos_por_status", "receita_total"),
                   ("resumo_clientes", "total_gasto")):
        if t in gold:
            _check_non_negative(gold[t], f"gold.{t}", col, report, WARN)
    return report


def counts_table(bronze: dict, silver: dict, gold: dict) -> pd.DataFrame:
    """Linhagem simples: linhas por tabela em cada camada."""
    rows = []
    for name in TABLE_NAMES:
        rows.append({"camada": "bronze", "tabela": name,
                     "linhas": len(bronze[name]) if name in bronze else None})
    for name in TABLE_NAMES:
        rows.append({"camada": "silver", "tabela": name,
                     "linhas": len(silver[name]) if name in silver else None})
    for name in GOLD_TABLES:
        rows.append({"camada": "gold", "tabela": name,
                     "linhas": len(gold[name]) if name in gold else None})
    return pd.DataFrame(rows)
