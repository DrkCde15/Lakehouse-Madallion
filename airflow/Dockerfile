FROM apache/airflow:2.10.5-python3.11

USER root

# Python do sistema na frente do PATH: build (pip sem o guard de root) e
# runtime (tasks como root) usam o mesmo interpretador /usr/local.
ENV PATH=/usr/local/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Prefetch dos JARs Maven (Delta + hadoop-aws) para a cache do Ivy —
# a primeira execução do pipeline não precisa de internet.
RUN python -c "from pyspark.sql import SparkSession; \
SparkSession.builder.appName('jar-warmup') \
.config('spark.jars.packages', 'io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4') \
.getOrCreate().stop()"
