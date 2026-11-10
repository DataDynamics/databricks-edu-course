-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 실습 3: 관리형 Delta 테이블로 주문 파이프라인 만들기
-- MAGIC 아래 카탈로그·스키마를 본인의 연습 공간으로 바꾸고 위에서부터 실행합니다.
-- MAGIC 이 노트북은 지정한 스키마의 edu_bronze_orders, edu_silver_orders,
-- MAGIC edu_gold_category_sales 테이블 내용을 매번 교체합니다.
-- MAGIC 검증 오류가 나면 후속 셀을 실행하지 말고 원인을 수정합니다.

-- COMMAND ----------

-- 두 이름은 자리표시자입니다. 기존 개인 연습 스키마를 사용합니다.
USE CATALOG training_catalog;
USE SCHEMA beginner_your_name;
SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- Bronze: 공통 CSV와 같은 현재 상태 스냅샷입니다.
CREATE OR REPLACE TABLE edu_bronze_orders
USING DELTA
AS SELECT * FROM VALUES
    (1, 'book', 12000, 'paid'),
    (2, 'book', 8000, 'paid'),
    (3, 'food', 15000, 'paid'),
    (4, 'food', 5000, 'cancelled'),
    (5, 'book', 7000, 'cancelled')
AS source(order_id, category, amount, status);

-- COMMAND ----------

-- assert_true는 조건이 참이면 NULL, 거짓/NULL이면 오류를 반환합니다.
SELECT
    assert_true(COUNT(*) = 5, '공통 샘플은 5행이어야 합니다.'),
    assert_true(COUNT(DISTINCT order_id) = COUNT(*), '주문 ID 중복 또는 누락'),
    assert_true(SUM(CASE WHEN
        order_id IS NULL OR order_id <= 0
        OR category IS NULL OR TRIM(category) = ''
        OR amount IS NULL OR amount < 0
        OR status IS NULL OR status NOT IN ('paid', 'cancelled')
        THEN 1 ELSE 0 END) = 0, '값 검증 실패')
FROM edu_bronze_orders;

-- COMMAND ----------

-- Silver: 검증된 상세 데이터. 취소 주문도 보존합니다.
CREATE OR REPLACE TABLE edu_silver_orders
USING DELTA
AS SELECT
    CAST(order_id AS BIGINT) AS order_id,
    category,
    CAST(amount AS BIGINT) AS amount,
    status
FROM edu_bronze_orders;

-- COMMAND ----------

-- Gold: 이번 질문에 필요한 결제 완료 금액만 집계합니다.
CREATE OR REPLACE TABLE edu_gold_category_sales
USING DELTA
AS SELECT category, SUM(amount) AS total_amount
FROM edu_silver_orders
WHERE status = 'paid'
GROUP BY category;

-- COMMAND ----------

SELECT 'bronze' AS layer, COUNT(*) AS row_count FROM edu_bronze_orders
UNION ALL
SELECT 'silver', COUNT(*) FROM edu_silver_orders
UNION ALL
SELECT 'gold', COUNT(*) FROM edu_gold_category_sales
ORDER BY layer;

-- COMMAND ----------

SELECT category, total_amount
FROM edu_gold_category_sales
ORDER BY category;

-- COMMAND ----------

SELECT assert_true(COUNT(*) = 5, 'Silver는 취소를 포함한 5행'),
       assert_true(SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) = 3,
                   '결제 완료는 3행')
FROM edu_silver_orders;

-- COMMAND ----------

SELECT assert_true(COUNT(*) = 2, 'Gold는 2행'),
       assert_true(SUM(total_amount) = 35000, '전체 금액 불일치'),
       assert_true(SUM(CASE
           WHEN category = 'book' AND total_amount = 20000 THEN 1
           WHEN category = 'food' AND total_amount = 15000 THEN 1
           ELSE 0 END) = 2, '카테고리별 금액 불일치')
FROM edu_gold_category_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 카탈로그에서 세 테이블을 확인하고 노트북을 처음부터 다시 실행합니다.
-- MAGIC Bronze 5행, Silver 5행, Gold 2행과 금액이 그대로인지 확인합니다.
-- MAGIC 실습 종료 후 컴퓨트 사용 상태를 확인합니다. 테이블 삭제는 안내 문서를 따릅니다.
