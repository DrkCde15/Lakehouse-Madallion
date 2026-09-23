# 🏠 Data Lakehouse - Arquitetura Medallion

Projeto de demonstração da arquitetura **Medallion** (Bronze → Silver → Gold) utilizando **Apache Spark**, **Delta Lake** e **MinIO** como lake de objetos S3-compatível.

---

## 📐 Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                           │
│           Agregações de negócio prontas para BI             │
├──────────────────────────────────────────────────────────────┤
│                       SILVER LAYER                           │
│      Dados limpos, tipados e deduplicados (single source)    │
├──────────────────────────────────────────────────────────────┤
│                       BRONZE LAYER                           │
│         Dados brutos ingeridos (full refresh/overwrite)      │
│            Cópia exata da fonte + coluna _ingested_at        │
└──────────────────────────────────────────────────────────────┘
```

### Camada Bronze (Raw)
- Ingestão dos CSVs de `data/` para tabelas **Delta Lake**
- Modo **overwrite** (full refresh a cada execução); histórico preservado pelo **Delta history**
- Sem transformação — apenas a coluna de auditoria `_ingested_at`

### Camada Silver (Curated)
- Deduplicação por chave natural (ex.: `order_id`, `customer_id`)
- Tipagem com `try_cast` (decimal, int, date) e normalização de texto
- Defaults para nulos (`"desconhecido"`, `"não informado"`)
- Timestamp `_ingested_at` convertido para America/Sao_Paulo

### Camada Gold (Business)
Quatro tabelas agregadas, prontas para BI:

| Tabela | Conteúdo |
|---|---|
| `gold.vendas_por_categoria` | Receita, itens e pedidos distintos por categoria |
| `gold.pedidos_por_status` | Contagem, receita e ticket médio por status |
| `gold.avaliacao_produto` | Média/mín/máx de notas por produto (sem fan-out) |
| `gold.resumo_clientes` | Gasto total, ticket médio e período de pedidos por cliente |

### Data Quality
Checks custom em PySpark (`src/dq/checks.py`) rodam **entre as camadas** como tasks do Airflow:

| Camada | Checks (exemplos) | Severidade |
|---|---|---|
| Bronze | tabela existe, não vazia, `_ingested_at` não nulo | ERROR |
| Silver | PK única (chave natural), `rating` 1–5, `status` no domínio, referencialidade (órfãos) | ERROR |
| Silver | valores negativos (`total_amount`, `payment_value`, `quantity`), `customer_id` sentinela | WARN |
| Gold | métricas não nulas, `avaliacao_media` 1–5, receitas não negativas | ERROR/WARN |

- **ERROR** → task falha e **bloqueia** a próxima camada
- **WARN** → só loga (os CSVs contêm casos reais negativos, reportados sem quebrar o pipeline)

---

## 🛠 Stack Tecnológica

| Componente       | Tecnologia         |
|------------------|--------------------|
| Processamento    | Apache Spark 3.x   |
| Formato de Dados | Delta Lake         |
| Lake de Objetos  | MinIO (S3)         |
| Orquestração     | Apache Airflow (Podman) |
| Linguagem        | Python 3.11+      |
| Ambiente Dev     | Jupyter Notebook   |

---

## 🚀 Pré-requisitos

- Python 3.11+
- Docker ou Podman (para MinIO)
- Java 11+ (para Spark)

---

## 📦 Instalação

```bash
# Criar ambiente virtual (Python 3.11+)
python3.11 -m venv .venv
source .venv/bin/activate

# Instalar dependências
pip install -r requirements.txt

# Configurar variáveis (Spark, Delta, MinIO, flags do projeto)
cp .env.example .env   # e ajuste se necessário

# Iniciar MinIO — via Podman Compose (recomendado, sobe junto com o Airflow)
podman-compose up -d minio

# ou manualmente:
podman run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

---

## ▶️ Como Rodar

A sessão Spark (Delta + hadoop-aws) é criada por `src/session.py`, com **todas as variáveis vindo do `.env`** (Spark, Delta, MinIO e flags).
**O padrão é MinIO**: os dados ficam no bucket `lake` (`s3a://lake/warehouse`), criado automaticamente se não existir.

```bash
source .venv/bin/activate

# Padrão (MinIO) — requer o container em http://localhost:9000
python -m src.ingestion.Bronze
python -m src.processing.Silver
python -m src.serving.Gold

# Modo local (opcional) — grava em ./spark-warehouse, não precisa do MinIO
USE_MINIO=0 python -m src.ingestion.Bronze
USE_MINIO=0 python -m src.processing.Silver
USE_MINIO=0 python -m src.serving.Gold

# Data quality (bronze|silver|gold) — exit 1 se houver ERROR
python -m src.dq.checks silver

# Notebook
jupyter lab notebooks/01_medallion_overview.ipynb

# Testes
pytest tests/ -v
```

Na primeira execução o Spark baixa os JARs de Delta Lake e hadoop-aws (requer internet).

---

## 🔁 Orquestração com Airflow (Podman)

O pipeline também roda orquestrado pelo **Apache Airflow**: 1 DAG (`medallion_pipeline`)
com 6 tasks sequenciais (`bronze_ingest >> check_bronze >> silver_process >> check_silver >> gold_aggregate >> check_gold`),
executando os jobs PySpark via `BashOperator` e os checks de data quality entre as camadas.
O compose sobe **Postgres** (metadata do Airflow), **MinIO** e o **Airflow**
(imagem custom com Java 17 + PySpark + Delta, JARs pré-baixados no build).

```bash
# Subir tudo
podman-compose up -d --build

# Airflow UI -> http://localhost:8080  (admin / admin)
# MinIO Console -> http://localhost:9001  (minioadmin / minioadmin)

# Disparar a DAG manualmente pela UI, ou via CLI:
podman exec medallion_airflow_scheduler airflow dags trigger medallion_pipeline

# Parar tudo
podman-compose down
```

- Agendamento: `@daily` com `catchup=False` e `max_active_runs=1` (o Derby metastore
  não aceita escrita concorrente — as tasks já rodam em sequência).
- **Endpoint S3A**: dentro dos containers é `http://minio:9000` (injetado pelo compose);
  no host continua `http://localhost:9000` (do `.env`). O `.env` não muda — o compose
  apenas sobrepõe essa variável no ambiente dos containers.
- Modo só-host (sem Airflow): `podman-compose up -d minio` e os comandos da seção acima.

---

## 📁 Estrutura do Projeto

```
08-data-lakehouse-medallion/
├── README.md
├── requirements.txt
├── docker-compose.yml               # Postgres + MinIO + Airflow (Podman)
├── .env.example                  # Template das variáveis (copie para .env)
├── data/                        # CSVs de entrada
│   ├── customers.csv
│   ├── order_items.csv
│   ├── orders.csv
│   ├── payments.csv
│   ├── products.csv
│   └── reviews.csv
├── airflow/
│   ├── Dockerfile                # Airflow + Java 17 + PySpark + Delta
│   ├── requirements.txt          # Dependências dos jobs Spark no container
│   └── dags/
│       └── medallion_pipeline.py # DAG: bronze >> silver >> gold (BashOperator)
├── src/
│   ├── __init__.py
│   ├── session.py               # SparkSession (Delta + S3A + conf)
│   ├── dq/
│   │   ├── __init__.py
│   │   └── checks.py            # Data quality (ERROR bloqueia / WARN loga)
│   ├── ingestion/
│   │   ├── __init__.py
│   │   └── Bronze.py
│   ├── processing/
│   │   ├── __init__.py
│   │   └── Silver.py
│   └── serving/
│       ├── __init__.py
│       └── Gold.py
├── notebooks/
│   └── 01_medallion_overview.ipynb
└── tests/
    ├── conftest.py               # Fixture da SparkSession de teste
    ├── test_silver.py            # Transforms da camada Silver
    ├── test_gold.py              # Agregações da camada Gold
    └── test_dq.py                # Checks de data quality
```

---

## 📝 Licença

MIT
