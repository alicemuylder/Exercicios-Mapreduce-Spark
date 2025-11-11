from typing import List, Tuple, Dict
from collections import defaultdict
import re, os

def map_function(document: str) -> List[Tuple[str, int]]:
    """Map: divide o texto em palavras e emite (palavra, 1)."""
    document = re.sub(r'[^a-zA-Z0-9\s]', '', document).lower()
    words = document.split()
    return [(word, 1) for word in words]

def shuffle_and_sort(mapped_values: List[Tuple[str, int]]) -> Dict[str, List[int]]:
    """Shuffle: agrupa valores por chave (palavra)."""
    grouped = defaultdict(list)
    for key, value in mapped_values:
        grouped[key].append(value)
    return dict(grouped)

def reduce_function(key: str, values: List[int]) -> Tuple[str, int]:
    """Reduce: soma as ocorrências de cada palavra."""
    return (key, sum(values))

def mapreduce(documents: List[str]) -> List[Tuple[str, int]]:
    """Executa o fluxo completo MapReduce."""
    mapped = []
    for doc in documents:
        mapped.extend(map_function(doc))
    grouped = shuffle_and_sort(mapped)
    reduced = [reduce_function(key, values) for key, values in grouped.items()]
    reduced.sort(key=lambda x: x[1], reverse=True)
    return reduced

if __name__ == "__main__":
    documents = [
        "Big Data is the future of technology",
        "Data Science uses Big Data analytics",
        "The future is Data driven and Big",
        "Big Data Analytics is important for business",
        "Machine Learning needs Big Data"
    ]

    print("=" * 60)
    print("EXERCÍCIO 1: WORDCOUNT COM MAPREDUCE")
    print("=" * 60)
    print("DISCIPLINA: Big Data e Cloud Computing — Prof. Cristiano Neto")
    print("ALUNA: Alice De Muylder Oliveira\n")

    results = mapreduce(documents)
    total_words = sum(count for _, count in results)

    print(f"Total de palavras únicas: {len(results)}")
    print(f"Total de palavras processadas: {total_words}")
    print(f"\nTop 15 palavras mais frequentes:")
    print("-" * 60)

    for i, (word, count) in enumerate(results[:15], 1):
        print(f"{i:2d}. {word:20s}: {count:3d}")

    # Salva resultados
    os.makedirs("results", exist_ok=True)
    with open("results/wordcount_results.txt", "w") as f:
        for word, count in results:
            f.write(f"{word}: {count}\n")
    print("\nResultados salvos em 'results/wordcount_results.txt'")
