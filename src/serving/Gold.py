"""Camada Gold: agregações de negócio prontas para BI."""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    max,
    min,
    sum,
)

from src.session import get_spark

logger = logging.getLogger(__name__)

SILVER_TABLES = ("payments", "customers", "orders", "order_items", "products", "reviews")


def load_silver(spark) -> dict:
    tables = {}
    for table_name in SILVER_TABLES:
        tables[table_name] = spark.table(f"silver.{table_name}")
        logger.info("silver.%s carregada | %d linhas", table_name, tables[table_name].count())
    return tables


def build_vendas_por_categoria(silver: dict):
    return (
        silver["order_items"]
        .join(silver["products"], on="product_id", how="left")
        .withColumn("receita", col("quantity") * col("unit_price"))
        .groupBy("category")
        .agg(
            sum("receita").alias("receita_total"),
            sum("quantity").alias("itens_vendidos"),
            countDistinct("product_id").alias("produtos_distintos"),
            countDistinct("order_id").alias("total_pedidos"),
        )
        .orderBy(col("receita_total").desc())
    )


def build_pedidos_por_status(silver: dict):
    return (
        silver["orders"]
        .groupBy("status")
        .agg(
            countDistinct("order_id").alias("total_pedidos"),
            sum("total_amount").alias("receita_total"),
            avg("total_amount").alias("ticket_medio"),
        )
        .orderBy(col("receita_total").desc())
    )


def build_avaliacao_produto(silver: dict):
    return (
        silver["reviews"]
        .join(silver["order_items"], on="order_id", how="left")
        .join(silver["products"], on="product_id", how="left")
        # Uma avaliação do pedido conta no máximo 1x por produto
        # (evita fan-out quando o pedido tem 2 itens do mesmo produto)
        .dropDuplicates(["review_id", "product_id"])
        .groupBy("product_id", "product_name", "category")
        .agg(
            avg("rating").alias("avaliacao_media"),
            countDistinct("review_id").alias("total_avaliacoes"),
            min("rating").alias("pior_nota"),
            max("rating").alias("melhor_nota"),
        )
        .orderBy(col("avaliacao_media").desc())
    )


def build_resumo_clientes(silver: dict):
    # Ticket médio por pedido (total_amount), consistente com gold.pedidos_por_status
    return (
        silver["orders"]
        .join(silver["customers"], on="customer_id", how="left")
        .groupBy("customer_id", "customer_name", "state", "city")
        .agg(
            countDistinct("order_id").alias("total_pedidos"),
            sum("total_amount").alias("total_gasto"),
            avg("total_amount").alias("ticket_medio"),
            min("order_date").alias("primeiro_pedido"),
            max("order_date").alias("ultimo_pedido"),
        )
        .orderBy(col("total_gasto").desc())
    )


GOLD_BUILDERS = {
    "vendas_por_categoria": build_vendas_por_categoria,
    "pedidos_por_status": build_pedidos_por_status,
    "avaliacao_produto": build_avaliacao_produto,
    "resumo_clientes": build_resumo_clientes,
}


def run(spark, show: bool = False):
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    logger.info("Carregando tabelas Silver para agregação...")
    silver = load_silver(spark)

    for table_name, builder in GOLD_BUILDERS.items():
        df = builder(silver)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.{table_name}")
        logger.info("gold.%s criada", table_name)
        if show:
            df.show(truncate=False)

    logger.info("Camada Gold concluída.")


def main():
    spark = get_spark("gold-aggregate")
    run(spark, show=True)


if __name__ == "__main__":
    main()
