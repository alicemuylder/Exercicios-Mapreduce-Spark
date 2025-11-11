from typing import List, Tuple, Dict
from collections import defaultdict, Counter
import re
import csv
import os

# ==================== DICIONÁRIOS DE SENTIMENTO ====================
POSITIVE_WORDS = {
    'excelente', 'ótimo', 'bom', 'maravilhoso', 'perfeito', 'adorei',
    'incrível', 'fantástico', 'recomendo', 'qualidade', 'satisfeito',
    'feliz', 'melhor', 'super', 'top', 'amei'
}

NEGATIVE_WORDS = {
    'ruim', 'péssimo', 'horrível', 'terrível', 'decepcionante', 'odiei',
    'defeito', 'problema', 'insatisfeito', 'pior', 'lixo', 'péssima',
    'nunca', 'não recomendo', 'arrependido', 'fraude'
}

# ==================== CLASSE REVIEW ====================
class Review:
    def __init__(self, product_id: str, rating: int, text: str):
        self.product_id = product_id
        self.rating = rating
        self.text = text.lower()

    def __repr__(self):
        return f"Review(product={self.product_id}, rating={self.rating})"

# ==================== UTILITÁRIOS ====================
def calculate_sentiment_score(text: str) -> float:
    words = re.findall(r'\b\w+\b', text)
    total_words = len(words)
    if total_words == 0:
        return 0.0
    pos_count = sum(1 for w in words if w in POSITIVE_WORDS)
    neg_count = sum(1 for w in words if w in NEGATIVE_WORDS)
    score = (pos_count - neg_count) / total_words
    return score

def classify_sentiment(score: float) -> str:
    if score > 0.1:
        return 'positivo'
    elif score < -0.1:
        return 'negativo'
    else:
        return 'neutro'

# ==================== FUNÇÕES MAPREDUCE ====================
def map_sentiment(review: Review) -> Tuple[str, int]:
    score = calculate_sentiment_score(review.text)
    sentiment = classify_sentiment(score)
    return (sentiment, 1)

def analyze_by_sentiment(reviews: List[Review]) -> Dict[str, int]:
    counts = defaultdict(int)
    for review in reviews:
        sentiment, one = map_sentiment(review)
        counts[sentiment] += one
    return dict(counts)

def map_product_sentiment(review: Review) -> Tuple[str, Tuple[str, int]]:
    sentiment, _ = map_sentiment(review)
    return (review.product_id, (sentiment, 1))

def reduce_product_sentiment(product_id: str, sentiments: List[Tuple[str, int]]) -> Dict:
    stats = defaultdict(int)
    for sentiment, count in sentiments:
        stats[sentiment] += count
    total = sum(stats.values())
    stats_pct = {k: f"{v} ({v/total*100:.1f}%)" for k, v in stats.items()}
    return {'product_id': product_id, 'total_reviews': total, 'sentiments': stats_pct}

def map_words_by_sentiment(review: Review) -> List[Tuple[Tuple[str, str], int]]:
    score = calculate_sentiment_score(review.text)
    sentiment = classify_sentiment(score)
    words = re.findall(r'\b\w+\b', review.text)
    return [((sentiment, w), 1) for w in words]

def compare_rating_sentiment(review: Review) -> Tuple[str, Dict]:
    score = calculate_sentiment_score(review.text)
    sentiment = classify_sentiment(score)
    if (review.rating >= 4 and sentiment == 'positivo') or \
       (review.rating <= 2 and sentiment == 'negativo'):
        return ('match', {'rating': review.rating, 'sentiment': sentiment})
    else:
        return ('mismatch', {'rating': review.rating, 'sentiment': sentiment})

# ==================== FUNÇÕES DE ARQUIVO ====================
def save_reviews_to_csv(reviews: List[Review], filename="reviews.csv"):
    os.makedirs("results", exist_ok=True)
    filepath = os.path.join("results", filename)
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "rating", "text"])
        for r in reviews:
            writer.writerow([r.product_id, r.rating, r.text])
    print(f"[OK] Arquivo salvo em: {filepath}")

def load_reviews_from_csv(filepath: str) -> List[Review]:
    reviews = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            reviews.append(Review(row["product_id"], int(row["rating"]), row["text"]))
    return reviews

def salvar_sentimentos_geral(dist: Dict[str, int], filename="ex3_sentiment_dist.txt"):
    filepath = os.path.join("results", filename)
    total = sum(dist.values())
    with open(filepath, "w", encoding="utf-8") as f:
        for sentiment, count in sorted(dist.items()):
            pct = (count / total) * 100
            f.write(f"{sentiment}: {count} ({pct:.1f}%)\n")
    print(f"[OK] Distribuição geral salva em: {filepath}")

def salvar_sentimentos_produto(product_map: Dict[str, List[Tuple[str, int]]], filename="ex3_sentiment_by_product.txt"):
    filepath = os.path.join("results", filename)
    with open(filepath, "w", encoding="utf-8") as f:
        for pid, s_list in product_map.items():
            stats = reduce_product_sentiment(pid, s_list)
            f.write(f"{pid}: {stats}\n")
    print(f"[OK] Sentimentos por produto salvos em: {filepath}")

def salvar_top_palavras(word_map: Dict[Tuple[str, str], int], filename="ex3_top_words.txt"):
    filepath = os.path.join("results", filename)
    counter = Counter(word_map)
    with open(filepath, "w", encoding="utf-8") as f:
        for sentiment in ['positivo', 'negativo', 'neutro']:
            f.write(f"\nSentimento {sentiment}:\n")
            top_words = [(w, c) for ((s, w), c) in counter.most_common() if s == sentiment][:5]
            for w, c in top_words:
                f.write(f"{w}: {c}\n")
    print(f"[OK] Top palavras por sentimento salvos em: {filepath}")

def salvar_concordancia(matches: Dict[str, int], filename="ex3_rating_vs_sentiment.txt"):
    filepath = os.path.join("results", filename)
    with open(filepath, "w", encoding="utf-8") as f:
        for status, count in matches.items():
            f.write(f"{status}: {count}\n")
    print(f"[OK] Concordância rating vs sentimento salva em: {filepath}")

# ==================== EXECUÇÃO PRINCIPAL ====================
if __name__ == "__main__":
    os.makedirs("results", exist_ok=True)
    csv_path = "results/reviews.csv"

    if os.path.exists(csv_path):
        reviews = load_reviews_from_csv(csv_path)
    else:
        reviews = [
            Review("PROD001", 5, "Produto excelente! Qualidade maravilhosa, super recomendo!"),
            Review("PROD001", 5, "Adorei! Melhor compra que já fiz. Perfeito!"),
            Review("PROD001", 4, "Muito bom, atendeu minhas expectativas"),
            Review("PROD002", 1, "Péssimo produto, qualidade horrível. Não recomendo!"),
            Review("PROD002", 2, "Decepcionante. Muitos defeitos e problemas."),
            Review("PROD002", 5, "Apesar dos reviews ruins, achei ótimo!"),
            Review("PROD003", 3, "Produto normal, nada de especial"),
            Review("PROD003", 3, "Nem bom nem ruim, mediano"),
            Review("PROD004", 5, "Incrível! Fantástico! Top demais!"),
            Review("PROD004", 1, "Pior compra da minha vida. Horrível!")
        ]
        save_reviews_to_csv(reviews)

    # === Análise 1: Distribuição geral de sentimentos ===
    sentiment_dist = analyze_by_sentiment(reviews)
    salvar_sentimentos_geral(sentiment_dist)

    # === Análise 2: Sentimentos por produto ===
    product_map = defaultdict(list)
    for review in reviews:
        pid, s = map_product_sentiment(review)
        product_map[pid].append(s)
    salvar_sentimentos_produto(product_map)

    # === Análise 3: Top palavras por sentimento ===
    word_map = defaultdict(int)
    for review in reviews:
        for key, count in map_words_by_sentiment(review):
            word_map[key] += count
    salvar_top_palavras(word_map)

    # === Análise 4: Concordância rating vs sentimento ===
    matches = defaultdict(int)
    for review in reviews:
        status, _ = compare_rating_sentiment(review)
        matches[status] += 1
    salvar_concordancia(matches)

    print("\n✅ Todas análises concluídas. Arquivos salvos em 'results/'")
