import time
import random
import csv
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# ==================== FUNÇÃO DE LOG PADRONIZADO ====================
def log_5_3(title: str):
    print("=" * 80)
    print(f"[5_3] {title}")

# ==================== GERAR DATASETS DE DIFERENTES TAMANHOS ====================
def generate_sales_dataset(file_path: str, num_rows: int):
    categories = ["Cat1", "Cat2", "Cat3", "Cat4", "Cat5"]
    products = [f"Product {chr(65 + i)}" for i in range(20)]
    sellers = [f"S{i}" for i in range(1, 21)]
    regions = ["North", "South", "East", "West"]
    start_time = datetime(2025, 1, 1)

    with open(file_path, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "product_id", "product_name", "category",
                         "quantity", "price", "seller_id", "region", "timestamp"])
        for i in range(num_rows):
            writer.writerow([
                f"T{i}",
                f"P{random.randint(1, 100)}",
                random.choice(products),
                random.choice(categories),
                random.randint(1, 10),
                round(random.uniform(10, 500), 2),
                random.choice(sellers),
                random.choice(regions),
                (start_time + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
            ])

# ==================== PYTHON PURO ====================
def sales_analysis_python(file_path: str):
    start = time.time()
    results = {}

    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        data = list(reader)

    # Cálculo total_value
    for row in data:
        row["total_value"] = float(row["price"]) * int(row["quantity"])

    # Top produtos
    product_revenue = {}
    for row in data:
        product_revenue[row["product_name"]] = product_revenue.get(row["product_name"], 0) + row["total_value"]
    results["top_products"] = sorted(product_revenue.items(), key=lambda x: x[1], reverse=True)[:10]

    # Receita por categoria
    category_revenue = {}
    for row in data:
        category_revenue[row["category"]] = category_revenue.get(row["category"], 0) + row["total_value"]
    results["category_revenue"] = category_revenue

    elapsed = time.time() - start
    return results, elapsed

# ==================== PYSPARK ====================
def sales_analysis_spark(sales_file: str, output_dir: str, cores: int = 1):
    os.makedirs(output_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Sales Analysis")
        .master(f"local[{cores}]")
        .getOrCreate()
    )

    try:
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("seller_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("timestamp", TimestampType(), True)
        ])

        start = time.time()

        sales_df = spark.read.option("header", "true").schema(schema).csv(sales_file)
        sales_df = sales_df.withColumn("total_value", col("quantity") * col("price"))
        sales_df.createOrReplaceTempView("sales")

        # Top 10 produtos
        log_5_3(f"TOP 10 PRODUTOS ({cores} cores)")
        top_products_df = spark.sql("""
            SELECT product_name, SUM(total_value) as revenue
            FROM sales
            GROUP BY product_name
            ORDER BY revenue DESC
            LIMIT 10
        """)
        top_products_df.show()
        top_products_df.write.mode("overwrite").parquet(os.path.join(output_dir, f"top_products_{cores}cores"))

        elapsed = time.time() - start
        return elapsed
    finally:
        spark.stop()

# ==================== TESTE COMPLETO ====================
if __name__ == "__main__":
    os.makedirs("results/output_sales_5_3", exist_ok=True)

    datasets = {
        "1MB": 10_000,
        "10MB": 100_000,
        "100MB": 1_000_000,
        # "1GB": 10_000_000  # cuidado, muito grande para teste rápido no Docker
    }

    results_summary = []

    for label, rows in datasets.items():
        file_name = f"sales_{label}.csv"
        log_5_3(f"Gerando dataset {label} com {rows} linhas...")
        generate_sales_dataset(file_name, rows)

        # Python puro
        log_5_3(f"Python puro - {label}")
        _, time_python = sales_analysis_python(file_name)

        # PySpark
        for cores in [1, 2, 4]:
            log_5_3(f"Spark ({cores} cores) - {label}")
            time_spark = sales_analysis_spark(file_name, os.path.join("results/output_sales_5_3", label), cores)
            speedup = time_python / time_spark if time_spark > 0 else 0

            print(f"\n===== RESULTADOS ({label} | {cores} cores) =====")
            print(f"Python Puro: {time_python:.2f}s")
            print(f"PySpark:     {time_spark:.2f}s")
            print(f"Speedup:     {speedup:.2f}x")

            results_summary.append((label, cores, time_python, time_spark, speedup))

    # ==================== GRÁFICOS DE COMPARAÇÃO ====================
    log_5_3("Gerando gráficos de performance...")
    labels = sorted(set(x[0] for x in results_summary))
    for cores in [1, 2, 4]:
        subset = [x for x in results_summary if x[1] == cores]
        sizes = [x[0] for x in subset]
        py_times = [x[2] for x in subset]
        sp_times = [x[3] for x in subset]

        plt.figure(figsize=(8,5))
        plt.plot(sizes, py_times, marker='o', label='Python Puro')
        plt.plot(sizes, sp_times, marker='o', label=f'PySpark ({cores} cores)')
        plt.title(f"Comparação de Performance ({cores} cores)")
        plt.xlabel("Tamanho do Dataset")
        plt.ylabel("Tempo (s)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join("results/output_sales_5_3", f"performance_{cores}cores.png"))
        plt.close()

    print("\nGráficos gerados em results/output_sales_5_3/")
