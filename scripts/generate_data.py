"""Gera os CSVs brutos da camada Bronze (data/*.csv) de forma deterministica.

Reproduz o schema e a sujeira proposital dos dados de exemplo — IDs prefixados
(O000001, C00442, PAY000001, P0014), duplicatas, caixa mista, valores negativos
e nulos — que as camadas Silver e o DQ treinam a resolver/reportar.

Uso:
    python -m scripts.generate_data                  # gera em ./data (padrao sujo)
    python -m scripts.generate_data --clean          # sem sujeira (DQ deve passar 100%)
    python -m scripts.generate_data --seed 7 --orders 5000 --out /tmp/data
"""

import argparse
import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OUT_DIR = ROOT / "data"

# Catalogo fixo dos 15 produtos (fiel aos dados de exemplo, acentos inclusos).
# Nota: P0004 com preco negativo e P0008 sem nome sao sujeira proposital.
PRODUCTS = [
    ("P0001", "Smartphone", "Eletrônicos", "899.9", 68),
    ("P0002", "Fone Bluetooth", "Eletrônicos", "149.9", 100),
    ("P0003", "Notebook", "Eletrônicos", "3499.9", 60),
    ("P0004", "Teclado Mecânico", "Informática", "-20.0", 27),
    ("P0005", "Mouse Gamer", "Informática", "179.9", 21),
    ("P0006", "Monitor 24", "Informática", "899.9", 53),
    ("P0007", "Cafeteira", "Casa", "399.9", 73),
    ("P0008", "", "Casa", "499.9", 95),
    ("P0009", "Aspirador", "Casa", "699.9", 34),
    ("P0010", "Tênis Esportivo", "Esportes", "349.9", 69),
    ("P0011", "Mochila", "Esportes", "199.9", 76),
    ("P0012", "Relógio Fitness", "Esportes", "299.9", 90),
    ("P0013", "Livro de Python", "Livros", "89.9", 50),
    ("P0014", "Livro de SQL", "Livros", "99.9", 14),
    ("P0015", "Livro de Data Engineering", "Livros", "149.9", 55),
]

CLEAN_PRODUCTS_OVERRIDES = {
    "P0004": {"price": "79.9"},
    "P0008": {"product_name": "Liquidificador"},
}

HEADERS = {
    "customers": ["customer_id", "customer_name", "email", "state", "city", "signup_date"],
    "orders": ["order_id", "customer_id", "order_date", "status", "total_amount"],
    "order_items": ["order_id", "item_id", "product_id", "quantity", "unit_price"],
    "payments": ["payment_id", "order_id", "payment_type", "payment_installments", "payment_value"],
    "products": ["product_id", "product_name", "category", "price", "stock"],
    "reviews": ["review_id", "order_id", "customer_id", "rating", "comment", "review_date"],
}

STATE_CITIES = [
    ("RJ", "Rio de Janeiro"), ("RJ", "Petrópolis"), ("SP", "São Paulo"),
    ("SP", "Campinas"), ("PR", "Curitiba"), ("MG", "Belo Horizonte"),
    ("RS", "Porto Alegre"), ("BA", "Salvador"), ("PE", "Recife"),
    ("CE", "Fortaleza"), ("SC", "Florianópolis"), ("GO", "Goiânia"),
]

STATUSES = [("delivered", 70), ("shipped", 13), ("processing", 11), ("cancelled", 6)]

PAYMENT_TYPES = [("credit_card", 26), ("debit_card", 26), ("boleto", 25), ("pix", 23)]

COMMENTS = [
    "Produto excelente", "Entrega rápida", "Poderia ser melhor",
    "Atendeu expectativas", "Chegou danificado", "Ótima qualidade",
    "Não recomendo", "Custo-benefício bom", "Exatamente como descrito",
    "Superou expectativas",
]

N_DIRTY_ORDER_DUPS = 10
N_DIRTY_NEGATIVE_ORDERS = 1
N_DIRTY_EMPTY_CUSTOMER = 1
N_DIRTY_EMPTY_STATE = 3
N_DIRTY_EMPTY_COMMENT = 5


def _weighted(rng: random.Random, pairs) -> str:
    values, weights = zip(*pairs)
    return rng.choices(values, weights=weights, k=1)[0]


def _rand_date(rng: random.Random, start: date, days: int) -> date:
    return start + timedelta(days=rng.randrange(days))


def _write_csv(path: Path, table: str, rows: list) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS[table])
        writer.writerows(rows)


def generate(
    out_dir: Path = OUT_DIR,
    seed: int = 42,
    n_customers: int = 508,
    n_orders: int = 1800,
    n_reviews: int = 1300,
    dirty: bool = True,
) -> dict:
    """Gera os 6 CSVs em out_dir e retorna {tabela: linhas_de_dados}."""
    rng = random.Random(seed)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------------- products (catalogo fixo) ----------------
    products = [list(p) for p in PRODUCTS]
    if not dirty:
        for pid, overrides in CLEAN_PRODUCTS_OVERRIDES.items():
            for row in products:
                if row[0] == pid:
                    if "price" in overrides:
                        row[3] = overrides["price"]
                    if "product_name" in overrides:
                        row[1] = overrides["product_name"]

    # ---------------- customers ----------------
    customers = []
    for i in range(1, n_customers + 1):
        state, city = rng.choice(STATE_CITIES)
        signup = _rand_date(rng, date(2024, 1, 1), 730)
        customers.append([
            f"C{i:05d}",
            f"Cliente {i:05d}",
            f"cliente{i:05d}@example.com",
            state,
            city,
            signup.isoformat(),
        ])

    # ---------------- orders + order_items ----------------
    orders = []
    items = []
    for i in range(1, n_orders + 1):
        oid = f"O{i:06d}"
        customer = rng.choice(customers)
        order_date = _rand_date(rng, date(2025, 1, 1), 365)
        total = 0.0
        for j in range(1, rng.randint(1, 4) + 1):
            product = rng.choice(products)
            qty = rng.randint(1, 5)
            unit = round(rng.uniform(50, 3500), 2)
            items.append([oid, f"{oid}-I{j}", product[0], qty, unit])
            total += qty * unit
        orders.append([
            oid,
            customer[0],
            order_date.isoformat(),
            _weighted(rng, STATUSES),
            round(total, 2),
        ])

    # ---------------- payments (1 por pedido) ----------------
    payments = []
    for i, order in enumerate(orders, start=1):
        payments.append([
            f"PAY{i:06d}",
            order[0],
            _weighted(rng, PAYMENT_TYPES),
            rng.randint(1, 12),
            order[4],
        ])

    # ---------------- reviews (amostra de pedidos) ----------------
    n_reviews = min(n_reviews, n_orders)
    reviewed = rng.sample(orders, n_reviews)
    reviews = []
    for i, order in enumerate(reviewed, start=1):
        order_date = date.fromisoformat(order[2])
        reviews.append([
            f"R{i:06d}",
            order[0],
            order[1],  # sempre um customer_id valido
            rng.randint(1, 5),
            rng.choice(COMMENTS),
            (order_date + timedelta(days=rng.randint(1, 14))).isoformat(),
        ])

    # ---------------- injecao de sujeira ----------------
    if dirty and n_orders > 0:
        idx_neg, idx_empty_cust = rng.sample(
            range(n_orders), min(N_DIRTY_NEGATIVE_ORDERS + N_DIRTY_EMPTY_CUSTOMER, n_orders)
        )
        if N_DIRTY_NEGATIVE_ORDERS:
            orders[idx_neg][4] = -100.0
            payments[idx_neg][4] = -50.0
        if N_DIRTY_EMPTY_CUSTOMER:
            orders[idx_empty_cust][1] = ""

        for i in rng.sample(range(n_orders), min(N_DIRTY_ORDER_DUPS, n_orders)):
            orders.append(list(orders[i]))

        for i in rng.sample(range(n_orders), min(1, n_orders)):
            orders[i][3] = "DELIVERED"

        for i in rng.sample(range(n_customers), min(N_DIRTY_EMPTY_STATE, n_customers)):
            customers[i][3] = ""

        if items:
            items[rng.randrange(len(items))][3] = -2

        if payments:
            payments[rng.randrange(len(payments))][2] = "PIX"

        with_comments = [i for i, r in enumerate(reviews) if r[4]]
        for i in rng.sample(with_comments, min(N_DIRTY_EMPTY_COMMENT, len(with_comments))):
            reviews[i][4] = ""

    counts = {
        "customers": len(customers),
        "orders": len(orders),
        "order_items": len(items),
        "payments": len(payments),
        "products": len(products),
        "reviews": len(reviews),
    }
    _write_csv(out_dir / "customers.csv", "customers", customers)
    _write_csv(out_dir / "orders.csv", "orders", orders)
    _write_csv(out_dir / "order_items.csv", "order_items", items)
    _write_csv(out_dir / "payments.csv", "payments", payments)
    _write_csv(out_dir / "products.csv", "products", products)
    _write_csv(out_dir / "reviews.csv", "reviews", reviews)
    return counts


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Gera os CSVs da camada Bronze")
    parser.add_argument("--out", type=Path, default=OUT_DIR, help="Diretorio de saida (default: ./data)")
    parser.add_argument("--seed", type=int, default=42, help="Seed do RNG (deterministico)")
    parser.add_argument("--customers", type=int, default=508)
    parser.add_argument("--orders", type=int, default=1800, help="Pedidos unicos (+10 duplicados se sujo)")
    parser.add_argument("--reviews", type=int, default=1300)
    parser.add_argument("--clean", action="store_true", help="Sem sujeira de exemplo")
    args = parser.parse_args(argv)

    counts = generate(
        out_dir=args.out,
        seed=args.seed,
        n_customers=args.customers,
        n_orders=args.orders,
        n_reviews=args.reviews,
        dirty=not args.clean,
    )
    for table, n in counts.items():
        print(f"{table}: {n} linhas -> {args.out / (table + '.csv')}")


if __name__ == "__main__":
    main()
