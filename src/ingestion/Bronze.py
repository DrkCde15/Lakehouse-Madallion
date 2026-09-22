"""Camada Bronze: ingestão dos CSVs brutos para tabelas Delta."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pyspark.sql.functions import current_timestamp

from src.session import get_spark

BRONZE_CSV_DIR = ROOT / "data"
TABLE_NAMES = ("payments", "customers", "orders", "order_items", "products", "reviews")


def read_csv(spark, table_name: str):
    path = BRONZE_CSV_DIR / f"{table_name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"CSV não encontrado: {path}")
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .load(str(path))
    )


def run(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    tables = {name: read_csv(spark, name) for name in TABLE_NAMES}

    for table_name, df in tables.items():
        full_table_name = f"bronze.{table_name}"
        (
            df
            .withColumn("_ingested_at", current_timestamp())
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(full_table_name)
        )
        print(f"✓ Tabela {full_table_name} salva com sucesso!")
        print(f"  Linhas: {df.count()} | Colunas: {len(df.columns)}")

    print("\n✅ Camada Bronze concluída! Todas as tabelas foram persistidas em formato Delta.")
    return tables


def main():
    spark = get_spark("bronze-ingest")
    run(spark)


if __name__ == "__main__":
    main()
