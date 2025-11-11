from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, hour, count, countDistinct
import os

# ==================== FUNÇÃO DE LOG PADRONIZADO ====================
def log_5_2(title: str, data=None):
    print("="*80)
    print(f"[5_2] {title}")
    if data is not None:
        if isinstance(data, list):
            for item in data:
                print(f"[5_2] {item}")
        else:
            print(f"[5_2] {data}")

# ==================== ANÁLISE DE LOGS ====================
def analyze_logs_spark(log_file: str, output_dir: str):
    spark = SparkSession.builder.appName("Log Analysis").getOrCreate()
    try:
        # criar pasta results se não existir
        os.makedirs(output_dir, exist_ok=True)

        logs_df = spark.read.text(log_file)
        parsed_logs = logs_df.select(
            regexp_extract('value', r'^(\S+)', 1).alias('ip'),
            regexp_extract('value', r'\[(.*?)\]', 1).alias('timestamp'),
            regexp_extract('value', r'"(\S+)', 1).alias('method'),
            regexp_extract('value', r'"\S+ (\S+)', 1).alias('url'),
            regexp_extract('value', r'" (\d{3})', 1).cast('int').alias('status'),
            regexp_extract('value', r'" \d{3} (\d+)', 1).cast('int').alias('size')
        ).withColumn('timestamp_dt', col('timestamp').cast('timestamp'))

        parsed_logs.createOrReplaceTempView("logs")

        # TOP 10 URLs
        log_5_2("TOP 10 URLs MAIS ACESSADAS")
        top_urls = spark.sql("""
            SELECT url, COUNT(*) as access_counta
            FROM logs
            WHERE url != ''
            GROUP BY url
            ORDER BY access_count DESC
            LIMIT 10
        """)
        top_urls.show(truncate=False)
        top_urls.write.mode("overwrite").parquet(os.path.join(output_dir, "top_urls"))

        # Distribuição de status HTTP
        log_5_2("DISTRIBUIÇÃO DE STATUS HTTP")
        status_dist = spark.sql("""
            SELECT status, COUNT(*) as count
            FROM logs
            GROUP BY status
            ORDER BY count DESC
        """)
        status_dist.show()
        status_dist.write.mode("overwrite").parquet(os.path.join(output_dir, "status_dist"))

        # Acessos por hora
        log_5_2("ACESSOS POR HORA")
        accesses_hour = parsed_logs.groupBy(hour("timestamp_dt").alias("hour")) \
            .count().orderBy("hour")
        accesses_hour.show()
        accesses_hour.write.mode("overwrite").parquet(os.path.join(output_dir, "accesses_hour"))

        # IPs únicos por hora
        log_5_2("IPs ÚNICOS POR HORA")
        unique_ips = parsed_logs.groupBy(hour("timestamp_dt").alias("hour")) \
            .agg(
                count("ip").alias("total_accesses"),
                countDistinct("ip").alias("unique_ips")
            ).orderBy("hour")
        unique_ips.show()
        unique_ips.write.mode("overwrite").parquet(os.path.join(output_dir, "unique_ips"))

    finally:
        spark.stop()

# ==================== TESTE ====================
if __name__ == "__main__":
    logs = """127.0.0.1 - - [24/Oct/2025:10:00:00] "GET /index.html HTTP/1.1" 200 1024
127.0.0.2 - - [24/Oct/2025:11:15:23] "POST /api/data HTTP/1.1" 500 2048
127.0.0.3 - - [24/Oct/2025:12:45:00] "GET /about.html HTTP/1.1" 404 512
""" * 20

    log_file = "access.log_5_2"
    with open(log_file, "w") as f:
        f.write(logs)

    analyze_logs_spark(log_file, "results/output_logs_5_2")
