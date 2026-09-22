from decimal import Decimal

from src.serving.Gold import (
    build_avaliacao_produto,
    build_pedidos_por_status,
    build_resumo_clientes,
    build_vendas_por_categoria,
)


def _silver(spark):
    orders = spark.createDataFrame(
        [
            ("1", "1", "2025-01-01", "delivered", "100.00"),
            ("2", "1", "2025-02-01", "delivered", "200.00"),
            ("3", "2", "2025-03-01", "canceled", "50.00"),
        ],
        ["order_id", "customer_id", "order_date", "status", "total_amount"],
    )
    customers = spark.createDataFrame(
        [("1", "Ana", "SP", "São Paulo"), ("2", "Bruno", "RJ", "Rio de Janeiro")],
        ["customer_id", "customer_name", "state", "city"],
    )
    # Pedido 1 tem 2 itens do MESMO produto (fan-out); pedido 2 também tem Eletrônicos
    order_items = spark.createDataFrame(
        [
            ("1", "1", "1", 1, "10.00"),
            ("1", "2", "1", 2, "10.00"),
            ("2", "3", "2", 1, "20.00"),
            ("2", "4", "1", 1, "10.00"),
            ("3", "5", "2", 1, "30.00"),
        ],
        ["order_id", "item_id", "product_id", "quantity", "unit_price"],
    )
    products = spark.createDataFrame(
        [
            ("1", "Notebook", "Eletrônicos", "10.00", 5),
            ("2", "Mouse", "Periféricos", "20.00", 9),
        ],
        ["product_id", "product_name", "category", "price", "stock"],
    )
    # 1 avaliação no pedido 1 — não deve contar 2x (2 itens do produto 1)
    reviews = spark.createDataFrame(
        [("1", "1", "1", 5, "ótimo", "2025-01-05")],
        ["review_id", "order_id", "customer_id", "rating", "comment", "review_date"],
    )
    payments = spark.createDataFrame(
        [("1", "1", "pix", "100.00")],
        ["payment_id", "order_id", "payment_type", "payment_value"],
    )
    return {
        "orders": orders,
        "customers": customers,
        "order_items": order_items,
        "products": products,
        "reviews": reviews,
        "payments": payments,
    }


def test_vendas_por_categoria_count_distinct_orders(spark):
    df = build_vendas_por_categoria(_silver(spark)).collect()
    by_cat = {row.category: row for row in df}

    # 3 linhas de item em Eletrônicos (2 no pedido 1 + 1 no pedido 2) → 2 pedidos distintos
    eletronicos = by_cat["Eletrônicos"]
    assert eletronicos.total_pedidos == 2  # pedidos 1 e 2 (distintos)
    assert eletronicos.itens_vendidos == 4  # 1 + 2 + 1
    assert eletronicos.receita_total == Decimal("40.00")  # 10 + 20 + 10


def test_pedidos_por_status(spark):
    df = {row.status: row for row in build_pedidos_por_status(_silver(spark)).collect()}

    assert df["delivered"].total_pedidos == 2
    assert df["delivered"].receita_total == Decimal("300.00")
    assert df["delivered"].ticket_medio == Decimal("150.00")
    assert df["canceled"].total_pedidos == 1


def test_avaliacao_produto_no_fan_out(spark):
    df = build_avaliacao_produto(_silver(spark)).collect()
    notebook = next(row for row in df if row.product_id == "1")

    # 1 review × 2 itens do mesmo produto → conta 1x
    assert notebook.total_avaliacoes == 1
    assert notebook.avaliacao_media == 5.0


def test_resumo_clientes_ticket_medio_per_order(spark):
    df = {row.customer_id: row for row in build_resumo_clientes(_silver(spark)).collect()}

    assert df["1"].total_pedidos == 2
    assert df["1"].total_gasto == Decimal("300.00")
    assert df["1"].ticket_medio == Decimal("150.00")  # média por pedido, não por item
