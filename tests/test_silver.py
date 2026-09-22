from decimal import Decimal

from pyspark.sql.types import StringType, StructField, StructType

from src.processing.Silver import (
    DEDUP_KEYS,
    move_column_to_end,
    normalize_timestamp_sp,
    transform_customers,
    transform_orders,
    transform_payments,
    transform_products,
)


def test_transform_payments_keeps_order_id(spark):
    df = spark.createDataFrame(
        [("PAY000001", "O000001", " PIX ", "1981.3")],
        ["payment_id", "order_id", "payment_type", "payment_value"],
    )
    row = transform_payments(df).collect()[0]

    assert row.order_id == "1"
    assert row.payment_id == "1"
    assert row.payment_type == "pix"
    assert row.payment_value == Decimal("1981.30")


def test_transform_orders_normalizes_status_and_ids(spark):
    df = spark.createDataFrame(
        [("O000001", "C00442", "2025-01-22", "DELIVERED", "1981.3")],
        ["order_id", "customer_id", "order_date", "status", "total_amount"],
    )
    row = transform_orders(df).collect()[0]

    assert row.order_id == "1"
    assert row.customer_id == "442"
    assert row.status == "delivered"
    assert row.total_amount == Decimal("1981.30")


def test_transform_orders_null_customer_becomes_default(spark):
    schema = StructType(
        [
            StructField(name, StringType())
            for name in ("order_id", "customer_id", "order_date", "status", "total_amount")
        ]
    )
    df = spark.createDataFrame(
        [("O000102", None, "2025-01-22", "shipped", "10.0")],
        schema=schema,
    )
    row = transform_orders(df).collect()[0]

    assert row.customer_id == "desconhecido"


def test_transform_customers_null_defaults(spark):
    schema = StructType(
        [
            StructField(name, StringType())
            for name in ("customer_id", "email", "state", "signup_date")
        ]
    )
    df = spark.createDataFrame(
        [("C00001", None, None, "2024-10-08")],
        schema=schema,
    )
    row = transform_customers(df).collect()[0]

    assert row.customer_id == "1"
    assert row.email == "não informado"
    assert row.state == "desconhecido"


def test_transform_products_keeps_decimal_price(spark):
    df = spark.createDataFrame(
        [("P0001", "Smartphone", "899.9", "68")],
        ["product_id", "product_name", "price", "stock"],
    )
    row = transform_products(df).collect()[0]

    assert row.product_id == "1"
    assert row.price == Decimal("899.90")
    assert row.stock == 68


def test_dedup_keys_cover_all_tables():
    assert set(DEDUP_KEYS) == {
        "payments",
        "customers",
        "orders",
        "order_items",
        "products",
        "reviews",
    }


def test_dedup_removes_duplicate_order_id(spark):
    df = spark.createDataFrame(
        [
            ("O1", "C1", "2025-01-01", "delivered", "10.0"),
            ("O1", "C1", "2025-01-01", "delivered", "10.0"),
            ("O2", "C1", "2025-01-02", "shipped", "20.0"),
        ],
        ["order_id", "customer_id", "order_date", "status", "total_amount"],
    )
    assert df.dropDuplicates(DEDUP_KEYS["orders"]).count() == 2


def test_move_column_to_end(spark):
    df = spark.createDataFrame([("a", "b")], ["col1", "_ingested_at"])
    assert move_column_to_end(df, "_ingested_at").columns == ["col1", "_ingested_at"]
    assert move_column_to_end(df, "col1").columns == ["_ingested_at", "col1"]


def test_normalize_timestamp_sp(spark):
    df = spark.createDataFrame([("2025-01-01 12:00:00",)], ["_ingested_at"])
    out = normalize_timestamp_sp(df)

    assert out.columns == ["_ingested_at"]
    assert out.collect()[0]._ingested_at is not None
