from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, desc
import os
import time

# ==================== FUNÇÃO DE LOG PADRONIZADO ====================
def log_5_1(title: str, data=None):
    print("="*80)
    print(f"[5_1] {title}")
    if data:
        if isinstance(data, list):
            for item in data:
                if isinstance(item, tuple):
                    print(f"[5_1] {item[0]:20s}: {item[1]}")
                else:
                    print(f"[5_1] {item}")
        else:
            print(f"[5_1] {data}")

# ==================== RDD API ====================
def wordcount_rdd(input_file: str, output_dir: str):
    sc = SparkContext("local[*]", "WordCount RDD")
    try:
        start_time = time.time()
        text_file = sc.textFile(input_file)

        counts = (text_file
                  .flatMap(lambda line: line.lower().replace(".", "").replace(",", "").split())
                  .map(lambda word: (word, 1))
                  .reduceByKey(lambda a, b: a + b)
                  .sortBy(lambda x: x[1], ascending=False))

        log_5_1("Top 20 palavras (RDD API)", counts.take(20))

        # criar pasta results se não existir
        os.makedirs("results/output_rdd_5_1", exist_ok=True)
        counts.saveAsTextFile("results/output_rdd_5_1")

        log_5_1(f"Tempo de execução RDD: {time.time() - start_time:.2f}s")
    finally:
        sc.stop()

# ==================== DataFrame API ====================
def wordcount_dataframe(input_file: str, output_dir: str):
    spark = SparkSession.builder.appName("WordCount DataFrame").getOrCreate()
    try:
        start_time = time.time()
        df = spark.read.text(input_file)

        word_counts = (df
                       .select(lower(col("value")).alias("line"))
                       .select(regexp_replace("line", "[^a-z ]", "").alias("line"))
                       .select(explode(split("line", " ")).alias("word"))
                       .filter(col("word") != "")
                       .groupBy("word")
                       .count()
                       .orderBy(desc("count")))

        log_5_1("Top 20 palavras (DataFrame API)")
        word_counts.show(20, truncate=False)

        # criar pasta results se não existir
        os.makedirs(output_dir, exist_ok=True)
        word_counts.write.mode("overwrite").parquet(output_dir)

        log_5_1(f"Tempo de execução DataFrame: {time.time() - start_time:.2f}s")
    finally:
        spark.stop()

# ==================== SQL API ====================
def wordcount_sql(input_file: str, output_dir: str):
    spark = SparkSession.builder.appName("WordCount SQL").getOrCreate()
    try:
        df = spark.read.text(input_file)
        df.createOrReplaceTempView("documents")

        word_counts = spark.sql("""
            SELECT word, COUNT(*) as count
            FROM (
                SELECT explode(split(REGEXP_REPLACE(LOWER(value), '[^a-z ]', ''), ' ')) as word
                FROM documents
            ) tmp
            WHERE word != ''
            GROUP BY word
            ORDER BY count DESC
        """)

        log_5_1("Top 20 palavras (SQL API)")
        word_counts.show(20, truncate=False)

        # criar pasta results se não existir
        os.makedirs(output_dir, exist_ok=True)
        word_counts.write.mode("overwrite").parquet(output_dir)
    finally:
        spark.stop()

# ==================== TESTE ====================
if __name__ == "__main__":
    test_text = """
    Big Data is the future of technology.
    Data Science uses Big Data analytics.
    The future is Data driven and Big.
    Big Data Analytics is important for business.
    Machine Learning needs Big Data.
    """ * 50

    input_file = "test_input_5_1.txt"
    with open(input_file, "w") as f:
        f.write(test_text)

    # rodar RDD
    wordcount_rdd(input_file, "results/output_rdd_5_1")

    # rodar DataFrame
    wordcount_dataframe(input_file, "results/output_dataframe_5_1")

    # rodar SQL
    wordcount_sql(input_file, "results/output_sql_5_1")
