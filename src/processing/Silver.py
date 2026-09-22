"""Camada Silver: limpeza, deduplicação, tipagem e normalização das tabelas Bronze."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql.functions import (
    col,
    expr,
    from_utc_timestamp,
    lit,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    when,
)

from src.session import get_spark

# Chave natural de cada tabela (deduplicação na Silver)
DEDUP_KEYS = {
    "payments": ["payment_id"],
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "item_id"],
    "products": ["product_id"],
    "reviews": ["review_id"],
}


def normalize_timestamp_sp(df, col_name="_ingested_at"):
    """Converte timestamp UTC para horário de São Paulo (America/Sao_Paulo)"""
    return df.withColumn(
        col_name,
        from_utc_timestamp(to_timestamp(col(col_name)), "America/Sao_Paulo")
    )


def move_column_to_end(df, col_name):
    """Move uma coluna para o final do DataFrame"""
    cols = [c for c in df.columns if c != col_name]
    return df.select(*cols, col_name)


def transform_payments(df):
    return (
        df.withColumn("payment_id", regexp_replace(col("payment_id"), "^PAY0*", ""))
        .withColumn("order_id", regexp_replace(col("order_id"), "^O0*", ""))
        .withColumn("payment_type", lower(trim(col("payment_type"))))
        .withColumn("payment_value", expr("try_cast(payment_value as decimal(10,2))"))
    )


def transform_customers(df):
    return (
        df.withColumn("customer_id", regexp_replace(col("customer_id"), "^C0*", ""))
        .withColumn("email", when(col("email").isNull(), lit("não informado")).otherwise(col("email")))
        .withColumn("state", when(col("state").isNull(), lit("desconhecido")).otherwise(col("state")))
        .withColumn("signup_date", to_date(col("signup_date")))
    )


def transform_orders(df):
    return (
        df.withColumn("order_id", regexp_replace(col("order_id"), "^O0*", ""))
        .withColumn("customer_id", when(col("customer_id").isNull(), lit("desconhecido"))
        .otherwise(regexp_replace(col("customer_id"), "^C0*", "")))
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("status", lower(trim(col("status"))))
        .withColumn("total_amount", expr("try_cast(total_amount as decimal(10,2))"))
    )


def transform_order_items(df):
    return (
        df.withColumn("order_id", regexp_replace(col("order_id"), "^O0*", ""))
        .withColumn("item_id", regexp_replace(col("item_id"), "^O0*", ""))
        .withColumn("product_id", regexp_replace(col("product_id"), "^P0*", ""))
        .withColumn("quantity", expr("try_cast(quantity as int)"))
        .withColumn("unit_price", expr("try_cast(unit_price as decimal(10,2))"))
    )


def transform_products(df):
    return (
        df.withColumn("product_id", regexp_replace(col("product_id"), "^P0*", ""))
        .withColumn("product_name", when(col("product_name").isNull(), lit("Produto sem nome"))
        .otherwise(col("product_name")))
        .withColumn("price", expr("try_cast(price as decimal(10,2))"))
        .withColumn("stock", expr("try_cast(stock as int)"))
    )


def transform_reviews(df):
    return (
        df.withColumn("review_id", regexp_replace(col("review_id"), "^R0*", ""))
        .withColumn("order_id", regexp_replace(col("order_id"), "^O0*", ""))
        .withColumn("customer_id", regexp_replace(col("customer_id"), "^C0*", ""))
        .withColumn("rating", expr("try_cast(rating as int)"))
        .withColumn("comment", when(col("comment").isNull(), lit("sem comentário"))
        .otherwise(col("comment")))
        .withColumn("review_date", to_date(col("review_date")))
    )


TABLE_TRANSFORMS = {
    "payments": transform_payments,
    "customers": transform_customers,
    "orders": transform_orders,
    "order_items": transform_order_items,
    "products": transform_products,
    "reviews": transform_reviews,
}


def run(spark, preview: bool = False):
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    for table_name, transform_fn in TABLE_TRANSFORMS.items():
        df = spark.table(f"bronze.{table_name}")
        df = df.dropDuplicates(DEDUP_KEYS[table_name])
        df = transform_fn(df)
        df = normalize_timestamp_sp(df)
        df = move_column_to_end(df, "_ingested_at")

        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"silver.{table_name}")
        print(f"✓ silver.{table_name} | {df.count()} linhas | {len(df.columns)} colunas")

    print("\nCamada Silver concluída!")

    if preview:
        for table_name in TABLE_TRANSFORMS:
            print(f"\n{'=' * 50}\nsilver.{table_name}\n{'=' * 50}")
            spark.table(f"silver.{table_name}").limit(10).show(truncate=False)


def main():
    spark = get_spark("silver-process")
    run(spark, preview=True)


if __name__ == "__main__":
    main()
