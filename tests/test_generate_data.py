"""Tests do gerador de dados (scripts/generate_data.py)."""

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_data import HEADERS, generate  # noqa: E402


def read_csv(path: Path) -> tuple[list, list]:
    with path.open(encoding="utf-8") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def test_headers_match_bronze_schema(tmp_path):
    generate(tmp_path, seed=1, n_customers=10, n_orders=20, n_reviews=10)
    for table, header in HEADERS.items():
        got, _ = read_csv(tmp_path / f"{table}.csv")
        assert got == header, table


def test_row_counts(tmp_path):
    counts = generate(tmp_path, seed=1, n_customers=10, n_orders=30, n_reviews=25)
    assert counts["customers"] == 10
    assert counts["orders"] == 30 + 10  # 10 duplicatas sujas
    assert counts["payments"] == 30
    assert counts["reviews"] == 25
    for table, n in counts.items():
        _, rows = read_csv(tmp_path / f"{table}.csv")
        assert len(rows) == n, table


def test_deterministic_with_same_seed(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(a, seed=42, n_customers=10, n_orders=30, n_reviews=20)
    generate(b, seed=42, n_customers=10, n_orders=30, n_reviews=20)
    for table in HEADERS:
        assert (a / f"{table}.csv").read_bytes() == (b / f"{table}.csv").read_bytes()


def test_different_seed_changes_data(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(a, seed=1, n_customers=10, n_orders=30, n_reviews=20)
    generate(b, seed=2, n_customers=10, n_orders=30, n_reviews=20)
    assert (a / "orders.csv").read_bytes() != (b / "orders.csv").read_bytes()


def test_dirty_injects_expected_sujeira(tmp_path):
    generate(tmp_path, seed=42, n_customers=50, n_orders=100, n_reviews=60)

    _, orders = read_csv(tmp_path / "orders.csv")
    order_ids = [r[0] for r in orders]
    assert len(order_ids) != len(set(order_ids)), "deve haver pedidos duplicados"
    assert any(r[4].startswith("-") for r in orders), "valor negativo"
    assert any(r[1] == "" for r in orders), "customer_id vazio"
    assert any(r[3] == "DELIVERED" for r in orders), "caixa alta"

    _, customers = read_csv(tmp_path / "customers.csv")
    assert any(r[3] == "" for r in customers), "state vazio"

    _, items = read_csv(tmp_path / "order_items.csv")
    assert any(int(r[3]) < 0 for r in items), "quantity negativa"

    _, payments = read_csv(tmp_path / "payments.csv")
    assert any(r[2] == "PIX" for r in payments), "payment_type caixa alta"
    assert any(float(r[4]) < 0 for r in payments), "payment_value negativo"

    _, reviews = read_csv(tmp_path / "reviews.csv")
    assert any(r[4] == "" for r in reviews), "comment vazio"


def test_clean_mode_has_no_sujeira(tmp_path):
    generate(tmp_path, seed=42, n_customers=50, n_orders=100, n_reviews=60, dirty=False)

    _, orders = read_csv(tmp_path / "orders.csv")
    order_ids = [r[0] for r in orders]
    assert len(order_ids) == len(set(order_ids))
    assert all(float(r[4]) >= 0 for r in orders)
    assert all(r[1] != "" for r in orders)
    assert all(r[3] == r[3].lower() for r in orders)

    _, items = read_csv(tmp_path / "order_items.csv")
    assert all(int(r[3]) > 0 for r in items)

    _, payments = read_csv(tmp_path / "payments.csv")
    assert all(float(r[4]) >= 0 for r in payments)
    assert all(r[2] == r[2].lower() for r in payments)

    _, customers = read_csv(tmp_path / "customers.csv")
    assert all(r[3] != "" for r in customers)


def test_referential_integrity(tmp_path):
    generate(tmp_path, seed=7, n_customers=40, n_orders=80, n_reviews=50)

    _, customers = read_csv(tmp_path / "customers.csv")
    _, orders = read_csv(tmp_path / "orders.csv")
    _, items = read_csv(tmp_path / "order_items.csv")
    _, payments = read_csv(tmp_path / "payments.csv")
    _, reviews = read_csv(tmp_path / "reviews.csv")
    _, products = read_csv(tmp_path / "products.csv")

    customer_ids = {r[0] for r in customers}
    order_ids = {r[0] for r in orders}
    product_ids = {r[0] for r in products}

    assert {r[1] for r in orders if r[1]} <= customer_ids
    assert {r[0] for r in items} <= order_ids
    assert {r[2] for r in items} <= product_ids
    assert {r[1] for r in payments} <= order_ids
    assert {r[1] for r in reviews} <= order_ids
    assert {r[2] for r in reviews} <= customer_ids


def test_id_formats_and_domains(tmp_path):
    generate(tmp_path, seed=7, n_customers=40, n_orders=80, n_reviews=50)

    _, customers = read_csv(tmp_path / "customers.csv")
    _, orders = read_csv(tmp_path / "orders.csv")
    _, items = read_csv(tmp_path / "order_items.csv")
    _, payments = read_csv(tmp_path / "payments.csv")
    _, reviews = read_csv(tmp_path / "reviews.csv")

    assert all(re.fullmatch(r"C\d{5}", r[0]) for r in customers)
    assert all(re.fullmatch(r"O\d{6}", r[0]) for r in orders)
    assert all(re.fullmatch(r"O\d{6}-I\d+", r[1]) for r in items)
    assert all(re.fullmatch(r"PAY\d{6}", r[0]) for r in payments)
    assert all(re.fullmatch(r"R\d{6}", r[0]) for r in reviews)
    assert all(r[3].lower() in {"delivered", "shipped", "processing", "cancelled"} for r in orders)
    assert all(1 <= int(r[3]) <= 5 for r in reviews)
