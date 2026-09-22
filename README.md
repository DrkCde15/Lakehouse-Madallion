# 🏠 Data Lakehouse - Arquitetura Medallion

Projeto de demonstração da arquitetura **Medallion** (Bronze → Silver → Gold) utilizando **Apache Spark**, **Delta Lake** e **MinIO** como lake de objetos S3-compatível.

---

## 📐 Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                        GOLD LAYER                           │
│              Dados agregados, dimensões e fatos              │
│            Tabelas de negócio prontas para BI                │
├──────────────────────────────────────────────────────────────┤
│                       SILVER LAYER                           │
│         Dados limpos, deduplicados e com schema             │
│             Versão única da verdade (Single Source)          │
├──────────────────────────────────────────────────────────────┤
│                       BRONZE LAYER                           │
│            Dados brutos ingeridos (append mode)             │
│            Cópia exata da fonte, sem transformação          │
└──────────────────────────────────────────────────────────────┘
```

### Camada Bronze (Raw)
- Ingestão de dados brutos de fontes externas (CSV, APIs, bancos de dados)
- Armazenamento em formato **Delta Lake** com modo **append**
- Sem transformação — preserva o estado original dos dados
- Permite reprocessamento e auditoria histórica

### Camada Silver (Curated)
- Limpeza: remoção de nulos, formatação de tipos, padronização
- Deduplicação: eliminação de registros duplicados
- Schema enforcement: garantia de consistência de esquema
- Enriquecimento e joins entre fontes

### Camada Gold (Business)
- Agregações de negócio (métricas, KPIs)
- Tabelas dimensionais (dim_tempo, dim_produto, dim_cliente)
- Tabelas fatos (vendas, transações)
- Prontas para consumo por ferramentas de BI e dashboards

---

## 🛠 Stack Tecnológica

| Componente       | Tecnologia         |
|------------------|--------------------|
| Processamento    | Apache Spark 3.x   |
| Formato de Dados | Delta Lake         |
| Lake de Objetos  | MinIO (S3)         |
| Linguagem        | Python 3.10+       |
| Ambiente Dev     | Jupyter Notebook   |

---

## 🚀 Pré-requisitos

- Python 3.10+
- Docker (para MinIO)
- Java 11+ (para Spark)

---

## 📦 Instalação

```bash
# Criar ambiente virtual (Python 3.10/3.11)
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
    └── __init__.py
```

---

## 📝 Licença

MIT
