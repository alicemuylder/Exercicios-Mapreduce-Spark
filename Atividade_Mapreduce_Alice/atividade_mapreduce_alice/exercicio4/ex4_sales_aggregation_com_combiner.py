import csv
import os
import random
from collections import defaultdict
from typing import List, Tuple, Dict

# ==================== GERAÇÃO DO CSV ====================
def gerar_transactions_csv(filepath: str, n: int = 200):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    products = [
        ("Café", "Alimentos", 10.00),
        ("Bolo", "Alimentos", 15.00),
        ("Leite", "Alimentos", 6.50),
        ("Camiseta", "Vestuário", 35.00),
        ("Calça", "Vestuário", 80.00),
        ("Notebook", "Eletrônicos", 3500.00),
        ("Fone de Ouvido", "Eletrônicos", 250.00),
        ("Cadeira Gamer", "Móveis", 1200.00),
        ("Mesa Office", "Móveis", 800.00),
        ("Luminária", "Casa", 120.00),
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "product", "category", "quantity", "price"])
        for i in range(n):
            product, category, price = random.choice(products)
            qty = random.randint(1, 5)
            writer.writerow([f"T{i+1:04}", product, category, qty, price])

def carregar_transactions(filepath: str) -> List[Dict[str, str]]:
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

def mapreduce_com_combiner(transactions: List[Dict[str, str]]) -> List[Tuple[str, float]]:
    combined = defaultdict(float)
    for t in transactions:
        combined[t["category"]] += float(t["quantity"]) * float(t["price"])
    reduced = [(k, v) for k, v in combined.items()]
    return sorted(reduced, key=lambda x: x[1], reverse=True)

def salvar_resultados(filepath: str, resultados: List[Tuple[str, float]]):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Categoria", "Receita_Total"])
        for cat, total in resultados:
            writer.writerow([cat, round(total, 2)])

if __name__ == "__main__":
    base_dir = os.path.dirname(__file__)
    csv_path = os.path.join(base_dir, "transactions.csv")
    results_dir = os.path.join(base_dir, "results")

    if not os.path.exists(csv_path):
        gerar_transactions_csv(csv_path, n=300)

    transactions = carregar_transactions(csv_path)
    resultado_com = mapreduce_com_combiner(transactions)
    salvar_resultados(os.path.join(results_dir, "resultado_com_combiner.csv"), resultado_com)

    print("\n--- RESULTADO COM COMBINER ---")
    for cat, total in resultado_com:
        print(f"{cat:15s} -> R$ {total:10.2f}")
