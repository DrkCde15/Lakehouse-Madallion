"""Testes do backend pandas do notebook (sem Spark/Java)."""

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import pandas_medallion as med


def test_strip_prefix_and_lower():
    """Verifica remoção de prefixos e normalização de texto."""
    df = pd.DataFrame([("PAY000001", "O000001", " PIX ")],
                      columns=["payment_id", "order_id", "payment_type"])
    df["payment_value"] = ["10.5"]
    out = med.transform_payments(df).iloc[0]
    assert out["payment_id"] == "1"
    assert out["order_id"] == "1"
    assert out["payment_type"] == "pix"
    assert float(out["payment_value"]) == 10.5


def test_empty_string_gets_default():
    """Verifica que string vazia recebe o valor default."""
    df = pd.DataFrame([("C00001", "", "")],
                      columns=["customer_id", "email", "state"])
    df["signup_date"] = ["2024-10-08"]
    out = med.transform_customers(df).iloc[0]
    assert out["customer_id"] == "1"
    assert out["email"] == "não informado"
    assert out["state"] == "desconhecido"


def test_silver_dedup_removes_duplicates():
    """Verifica que a Silver remove os 10 duplicados do gerador."""
    bronze = med.load_bronze()
    n_bronze = len(bronze["orders"])
    silver = med.run_silver(bronze)
    assert len(silver["orders"]) == n_bronze - 10  # sujeira proposital do gerador


def test_gold_avaliacao_no_fan_out():
    # Pedido 1 tem 2 itens do MESMO produto + 1 review -> conta 1x
    """Verifica que 1 review conta 1x mesmo com 2 itens do produto."""
    silver = {
        "reviews": pd.DataFrame(
            [("1", "1", "1", 5, "ótimo", "2025-01-05")],
            columns=["review_id", "order_id", "customer_id", "rating", "comment", "review_date"]),
        "order_items": pd.DataFrame(
            [("1", "1", "1", 1, 10.0), ("1", "2", "1", 2, 10.0)],
            columns=["order_id", "item_id", "product_id", "quantity", "unit_price"]),
        "products": pd.DataFrame(
            [("1", "Notebook", "Eletrônicos", 10.0, 5)],
            columns=["product_id", "product_name", "category", "price", "stock"]),
    }
    out = med.build_avaliacao_produto(silver)
    assert len(out) == 1
    assert int(out.iloc[0]["total_avaliacoes"]) == 1
    assert float(out.iloc[0]["avaliacao_media"]) == 5.0


def test_dq_reports_ok_with_only_warns():
    """Verifica que o DQ aprova com apenas WARNs de negativos."""
    bronze = med.load_bronze()
    silver = med.run_silver(bronze)
    gold = med.run_gold(silver)
    assert med.run_dq_bronze(bronze).ok
    r_silver = med.run_dq_silver(silver)
    assert r_silver.ok  # WARNs de negativos não bloqueiam
    assert r_silver.warnings
    assert med.run_dq_gold(gold).ok


def test_module_has_no_pyspark_dependency():
    """Verifica que o módulo pandas não importa PySpark/Delta."""
    src = (ROOT / "src" / "pandas_medallion.py").read_text(encoding="utf-8")
    for forbidden in ("import pyspark", "from pyspark", "import delta", "from delta",
                      "get_spark"):
        assert forbidden not in src
