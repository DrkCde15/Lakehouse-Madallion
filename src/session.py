"""Factory de SparkSession com Delta Lake, hadoop-aws (S3A) e config do projeto.

Todas as variáveis (Spark, Delta, MinIO e flags do projeto) vivem no `.env`
(template em `.env.example`). Nada de config em arquivos .conf.
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
)


def _truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def read_spark_env() -> dict:
    """Retorna as variáveis spark.* do ambiente (carregadas do .env)."""
    return {k: v for k, v in os.environ.items() if k.startswith("spark.")}


def ensure_minio_bucket(props: dict) -> str:
    """Cria o bucket no MinIO se não existir e retorna o nome."""
    from minio import Minio

    bucket = os.environ.get("MINIO_BUCKET", "lake")
    endpoint = props.get("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    endpoint = endpoint.split("://", 1)[-1]
    secure = _truthy(props.get("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"))
    client = Minio(
        endpoint,
        access_key=props.get("spark.hadoop.fs.s3a.access.key", "minioadmin"),
        secret_key=props.get("spark.hadoop.fs.s3a.secret.key", "minioadmin"),
        secure=secure,
    )
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Bucket '%s' criado no MinIO", bucket)
    else:
        logger.info("Bucket '%s' já existe no MinIO", bucket)
    return bucket


def get_spark(app_name: str = "DataLakehouseMedallion", use_minio: bool | None = None) -> SparkSession:
    """Cria a SparkSession com Delta + S3A.

    - use_minio=None (padrão): lê USE_MINIO do .env (padrão: true — só MinIO).
      USE_MINIO=0 força o modo local (./spark-warehouse).
    - Modo MinIO: warehouse em s3a://<bucket>/warehouse e cria o bucket se faltar.
    - Modo local: warehouse em ./spark-warehouse (não precisa do MinIO).
    """
    if use_minio is None:
        use_minio = _truthy(os.environ.get("USE_MINIO", "true"))

    props = read_spark_env()
    builder = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.jars.packages", ",".join(_PACKAGES))
    )
    for key, value in props.items():
        builder = builder.config(key, value)

    if use_minio:
        bucket = ensure_minio_bucket(props)
        builder = (
            builder
            .config("spark.sql.warehouse.dir", f"s3a://{bucket}/warehouse")
            # Metastore separado do modo local (não mistura paths s3a com ./spark-warehouse)
            .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_minio;create=true")
        )
        logger.info("Storage: MinIO (s3a://%s/warehouse)", bucket)
    else:
        warehouse = ROOT / "spark-warehouse"
        builder = (
            builder
            .config("spark.sql.warehouse.dir", str(warehouse))
            .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true")
        )
        logger.info("Storage: local (%s)", warehouse)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
