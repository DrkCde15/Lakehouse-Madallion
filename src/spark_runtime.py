"""Alinha SPARK_HOME e PYSPARK_* à versão do pyspark instalado.

SPARK_HOME apontando para outra release (ex.: spark-3.5.9 com pyspark==3.5.1)
deixa o JVM e o client Python fora de sincronia: o capability check do Delta
rejeita saveAsTable(mode="overwrite") com "does not support truncate in batch
mode" (delta-io/delta#4671). PYSPARK_PYTHON desalinhado derruba os workers de
Python com PYTHON_VERSION_MISMATCH.

Chamado por src.session e por tests/conftest.py — cada um cria a própria
SparkSession, então a correção precisa existir nos dois caminhos.
"""

import glob
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

import pyspark

logger = logging.getLogger(__name__)


def _python_version(executable: str) -> tuple[int, ...] | None:
    """Retorna (major, minor) do interpretador, ou None se não executável."""
    try:
        out = subprocess.run(
            [executable, "-c", "import sys; print(*sys.version_info[:2])"],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
        return tuple(int(part) for part in out.stdout.split())
    except Exception:
        return None


def _jvm_version(spark_home: str) -> str | None:
    """Lê a versão do Spark a partir do SPARK_HOME (jar spark-sql ou dirname)."""
    jars = glob.glob(str(Path(spark_home) / "jars" / "spark-sql_2.12-*.jar"))
    match = re.search(r"spark-sql_2\.12-(\d+\.\d+\.\d+)\.jar", jars[0]) if jars else None
    if match is None:
        match = re.search(r"spark-(\d+\.\d+\.\d+)", spark_home)
    return match.group(1) if match else None


def align_spark_runtime() -> None:
    """Faz o runtime do Spark bater com o pyspark instalado. Idempotente."""
    py_version = pyspark.__version__
    spark_home = os.environ.get("SPARK_HOME")
    jvm_version = _jvm_version(spark_home) if spark_home else None

    if jvm_version != py_version:
        bundled = Path(pyspark.__file__).resolve().parent
        logger.warning(
            "SPARK_HOME=%s tem Spark %s mas o pyspark é %s — usando %s",
            spark_home,
            jvm_version or "desconhecida",
            py_version,
            bundled,
        )
        os.environ["SPARK_HOME"] = str(bundled)

    for var in ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON"):
        current = os.environ.get(var)
        if current is None:
            logger.warning("%s não está definida — usando %s", var, sys.executable)
            os.environ[var] = sys.executable
        elif _python_version(current) != sys.version_info[:2]:
            logger.warning(
                "%s=%s não roda o Python do driver (%s) — usando %s",
                var, current, sys.version.split()[0], sys.executable,
            )
            os.environ[var] = sys.executable
