"""Factory de SparkSession com Delta Lake, hadoop-aws (S3A) e config do projeto."""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CONF_PATH = ROOT / "config" / "spark-defaults.conf"

_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
)


def _truthy(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def read_spark_conf(path: Path = CONF_PATH) -> dict:
    """Lê spark-defaults.conf (key<espaço>value, '#' comenta)."""
    props: dict = {}
    if not path.exists():
        return props
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) == 2:
            props[parts[0]] = parts[1]
    return props


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
        print(f"✓ Bucket '{bucket}' criado no MinIO")
    else:
        print(f"✓ Bucket '{bucket}' já existe no MinIO")
    return bucket


def get_spark(app_name: str = "DataLakehouseMedallion", use_minio: bool | None = None) -> SparkSession:
    """Cria a SparkSession com Delta + S3A.

    - use_minio=None (padrão): lê USE_MINIO (padrão: true — só MinIO).
      USE_MINIO=0 força o modo local (./spark-warehouse).
    - Modo MinIO: warehouse em s3a://<bucket>/warehouse e cria o bucket se faltar.
    - Modo local: warehouse em ./spark-warehouse (não precisa do MinIO).
    """
    if use_minio is None:
        use_minio = _truthy(os.environ.get("USE_MINIO", "true"))

    props = read_spark_conf()
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
        print(f"Storage: MinIO (s3a://{bucket}/warehouse)")
    else:
        warehouse = ROOT / "spark-warehouse"
        builder = (
            builder
            .config("spark.sql.warehouse.dir", str(warehouse))
            .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db;create=true")
        )
        print(f"Storage: local ({warehouse})")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
