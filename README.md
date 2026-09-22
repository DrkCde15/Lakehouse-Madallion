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

---

## 🛠 Stack Tecnológica

| Componente       | Tecnologia         |
|------------------|--------------------|
| Processamento    | Apache Spark 3.x   |
| Formato de Dados | Delta Lake         |
| Lake de Objetos  | MinIO (S3)         |
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

# Iniciar MinIO (obrigatório por padrão) — Docker ou Podman
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# ou com Podman:
podman run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

---

## ▶️ Como Rodar

A sessão Spark (Delta + hadoop-aws + `config/spark-defaults.conf`) é criada por `src/session.py`.
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

# Notebook
jupyter lab notebooks/01_medallion_overview.ipynb

# Testes
pytest tests/ -v
```

Na primeira execução o Spark baixa os JARs de Delta Lake e hadoop-aws (requer internet).

---

## 📁 Estrutura do Projeto

```
08-data-lakehouse-medallion/
├── README.md
├── requirements.txt
├── config/
│   └── spark-defaults.conf
├── data/                        # CSVs de entrada
│   ├── customers.csv
│   ├── order_items.csv
│   ├── orders.csv
│   ├── payments.csv
│   ├── products.csv
│   └── reviews.csv
├── src/
│   ├── __init__.py
│   ├── session.py               # SparkSession (Delta + S3A + conf)
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
    └── test_gold.py              # Agregações da camada Gold
```

---

## 📝 Licença

MIT
