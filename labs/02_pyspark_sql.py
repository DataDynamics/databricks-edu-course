# Databricks notebook source
# MAGIC %md
# MAGIC # 실습 2: PySpark와 SQL로 같은 주문 집계
# MAGIC 노트북용 Python/Spark 컴퓨트에 연결하고 위에서부터 실행합니다.
# MAGIC 테이블을 저장하지 않습니다. 공통 CSV와 같은 5행을 셀에서 생성합니다.

# COMMAND ----------

from pyspark.sql import functions as F

# Databricks 노트북이 제공하는 spark(SparkSession)를 사용합니다.
orders = [
    (1, "book", 12000, "paid"),
    (2, "book", 8000, "paid"),
    (3, "food", 15000, "paid"),
    (4, "food", 5000, "cancelled"),
    (5, "book", 7000, "cancelled"),
]
orders_df = spark.createDataFrame(
    orders,
    schema="order_id INT, category STRING, amount LONG, status STRING",
)
orders_df.printSchema()
orders_df.orderBy("order_id").show()

# COMMAND ----------

# Transformation: 아직 집계 결과를 Python으로 가져오지 않습니다.
sales_df = (
    orders_df
    .filter(F.col("status") == "paid")
    .groupBy("category")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("category")
)
sales_df.explain()  # 계획 확인; 출력 모양은 실행 환경에 따라 다릅니다.

# COMMAND ----------

# Action: 결과를 표시합니다.
sales_df.show()

# COMMAND ----------

# 세션 안에서만 사용하는 SQL 이름입니다. 영구 테이블이 아닙니다.
orders_df.createOrReplaceTempView("orders")
sql_sales_df = spark.sql("""
    SELECT category, SUM(amount) AS total_amount
    FROM orders
    WHERE status = 'paid'
    GROUP BY category
    ORDER BY category
""")
sql_sales_df.show()

# COMMAND ----------

# 공통 샘플의 집계 결과는 두 행이므로 여기서는 collect()로 비교합니다.
# 대규모 원본을 전체 수집하는 패턴으로 사용하지 않습니다.
expected = [("book", 20000), ("food", 15000)]
pyspark_result = [(row.category, row.total_amount) for row in sales_df.collect()]
sql_result = [(row.category, row.total_amount) for row in sql_sales_df.collect()]
assert orders_df.count() == 5
assert pyspark_result == sql_result == expected
print("PySpark·SQL 결과 일치: book=20000, food=15000")
