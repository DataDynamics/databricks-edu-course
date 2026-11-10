# Python과 PySpark의 차이 이해하기

이 문서는 Databricks를 처음 접하는 개발자를 위한 입문 자료입니다. Python의 변수, 리스트, 반복문, 함수에 대한 기초 지식을 전제로 합니다. 설명과 실습에는 약 30분이 필요합니다.

## 학습 목표

이 문서를 학습하면 다음 내용을 설명할 수 있습니다.

- Python, Apache Spark, PySpark, Databricks의 관계를 설명합니다.
- 일반적인 Python 코드와 PySpark DataFrame 코드의 실행 방식 차이를 구분합니다.
- 같은 데이터를 두 방식으로 필터링하고 집계합니다.
- 지연 실행과 분산 환경에서 발생하기 쉬운 실수를 이해합니다.

## 1. Python은 언어이고, PySpark는 Python으로 Spark를 사용하는 API입니다

**PySpark는 Python을 대체하는 별도의 프로그래밍 언어가 아닙니다.** PySpark 코드도 Python 문법으로 작성합니다. 차이는 데이터 처리 작업을 어떤 도구에 맡기고, 어디에서 실행하는지에 있습니다.

| 이름 | 의미 | 이 교육에서의 역할 |
| --- | --- | --- |
| Python | 범용 프로그래밍 언어입니다. | 변수, 조건문, 반복문, 함수 등 프로그램을 작성합니다. |
| Apache Spark | 대규모 데이터 처리를 위한 엔진입니다. | 데이터를 나누어 처리하고 실행 계획을 최적화합니다. |
| PySpark | Apache Spark의 Python API입니다. | Python 코드로 Spark에 데이터 처리 작업을 요청합니다. |
| Databricks | Spark 기반 데이터 처리 등을 지원하는 플랫폼입니다. | 노트북과 컴퓨팅 환경에서 코드를 작성하고 실행합니다. |

Python은 웹 개발, 자동화 등에도 사용하는 언어이며, PySpark는 그 Python에서 가져다 쓰는 데이터 처리 도구입니다. Python과 PySpark의 정의는 [Python 공식 튜토리얼](https://docs.python.org/3/tutorial/)과 [PySpark 공식 개요](https://spark.apache.org/docs/latest/api/python/)에서 확인할 수 있습니다.

Databricks의 Python 노트북에서는 일반 Python 코드와 PySpark 코드를 함께 사용합니다. 노트북에서 Python을 선택했다고 해서 모든 코드가 자동으로 Spark 분산 작업이 되는 것은 아닙니다. 노트북의 언어 사용 방식은 [Databricks 공식 문서](https://docs.databricks.com/aws/en/notebooks/notebooks-code)를 참고합니다.

> 이 문서의 “일반 Python 처리”는 리스트와 반복문 등으로 데이터를 직접 처리하는 예제를 뜻합니다. Python 자체가 단일 컴퓨터나 단일 스레드로만 실행된다는 뜻은 아닙니다. Python에서도 다른 라이브러리나 프로세스를 이용해 병렬·분산 처리를 구현할 수 있습니다. Spark 역시 한 대의 컴퓨터에서 로컬 모드로 실행할 수 있습니다.

## 2. 가장 큰 차이는 데이터 처리의 실행 방식입니다

![일반 Python 예제는 한 프로세스에서 데이터를 처리합니다. PySpark는 처리 계획을 Spark에 전달하며, Driver가 작업을 조율하고 Executor들이 파티션을 병렬로 처리합니다.](images/python-vs-pyspark.png)

그림의 왼쪽은 리스트를 순회하며 직접 계산하는 예제입니다. 오른쪽은 PySpark로 요청한 작업을 Spark가 여러 실행 프로세스에 나누어 처리하는 개념도입니다. 화살표는 요청과 작업 배분을 나타내며, 원본 데이터 전체가 Driver를 거쳐 이동한다는 뜻은 아닙니다.

분산 처리를 이해하려면 다음 용어를 먼저 알아야 합니다.

| 용어 | 의미 |
| --- | --- |
| 클러스터(Cluster) | 여러 컴퓨터의 자원을 모아 작업을 실행하는 환경입니다. |
| 드라이버(Driver) | Spark 애플리케이션의 실행을 조율하고 작업을 배분하는 프로세스입니다. |
| 실행기(Executor) | 배정된 작업을 실행하고 처리 중인 데이터를 관리하는 프로세스입니다. |
| 파티션(Partition) | 데이터를 나누어 처리하기 위한 논리적인 단위입니다. |

하나의 Executor가 여러 파티션에 대한 작업을 처리할 수 있습니다. 파티션 수, Executor 수, 컴퓨터 수는 서로 같은 개념이 아닙니다. 구성 요소의 관계는 [Spark 클러스터 개요](https://spark.apache.org/docs/latest/cluster-overview.html)를 참고합니다.

| 비교 항목 | 일반 Python 처리 예제 | PySpark DataFrame 처리 |
| --- | --- | --- |
| 개발자가 작성하는 내용 | 각 항목을 어떻게 처리할지 작성합니다. | 어떤 열을 선택하고 어떤 조건으로 집계할지 표현합니다. |
| 주요 데이터 구조 | 리스트, 딕셔너리 등을 사용합니다. | 행과 열로 구성된 Spark DataFrame을 사용합니다. |
| 실행 주체 | 예제 코드를 실행하는 Python 프로세스가 계산합니다. | Spark가 계획을 최적화하고 작업을 배분합니다. |
| 계산 시점 | 예제의 반복문을 실행할 때 계산합니다. | 변환을 계획하고 액션을 호출할 때 필요한 계산을 수행합니다. |
| 데이터 규모 | 예제처럼 데이터를 리스트에 담으면 해당 프로세스의 메모리를 사용합니다. | 파티션별로 나누어 여러 실행 자원을 활용할 수 있습니다. |
| 성능 고려 사항 | 알고리즘, 메모리 사용량 등을 고려합니다. | 실행 계획, 파티션, 데이터 이동, 자원 사용량도 고려합니다. |

표의 실행 시점 비교는 아래 예제를 기준으로 합니다. Python에도 제너레이터처럼 계산을 미루는 기능이 있으므로, “Python은 항상 즉시 실행하고 PySpark는 항상 모든 것을 미룬다”라고 일반화하지 않습니다.

### pandas와 PySpark DataFrame도 구분해야 합니다

pandas는 Python에서 표 형태의 데이터를 다루는 라이브러리입니다. 일반적인 pandas DataFrame은 한 컴퓨터의 메모리를 중심으로 처리합니다. PySpark DataFrame은 분산 처리할 수 있는 데이터와 처리 계획을 표현합니다.

두 객체는 이름에 `DataFrame`이 들어가지만 메서드와 실행 방식이 다릅니다. 또한 `pyspark.pandas`로 사용하는 **pandas API on Spark**는 일반 pandas와 구분해야 합니다. 이 문서는 `pyspark.sql`의 DataFrame API를 다룹니다. API별 역할은 [PySpark 공식 개요](https://spark.apache.org/docs/latest/api/python/)에서 확인할 수 있습니다.

## 3. 같은 문제를 Python과 PySpark로 해결합니다

실습에서는 **결제 완료된 주문만 선택하여 카테고리별 매출 합계를 계산**합니다. 금액은 계산을 단순하게 하기 위해 정수로 표현합니다.

| 주문 번호 | 카테고리 | 금액 | 상태 |
| --- | --- | ---: | --- |
| 1 | book | 12000 | paid |
| 2 | book | 8000 | paid |
| 3 | food | 15000 | paid |
| 4 | food | 5000 | cancelled |
| 5 | book | 7000 | cancelled |

### 실습 환경을 준비합니다

Databricks에서 Python 노트북을 만들고 Python과 Spark를 실행할 수 있는 컴퓨팅에 연결합니다. 아래 셀을 순서대로 실행합니다. 예제는 외부 파일이나 별도 테이블 권한 없이 사용할 수 있도록 작은 데이터를 코드에 직접 정의합니다.

Databricks 노트북에서는 제공되는 `spark` 객체를 사용합니다. 이 객체는 Spark 기능을 호출하는 진입점인 `SparkSession`입니다. 이 실습에서는 별도로 PySpark를 설치하거나 `SparkSession`을 만들지 않습니다. 기본 환경과 실행 흐름은 [Databricks Spark 시작하기](https://www.databricks.com/spark/getting-started-with-apache-spark/quick-start)를 참고합니다.

### 셀 1: 공통 데이터를 정의합니다

```python
orders = [
    (1, "book", 12000, "paid"),
    (2, "book", 8000, "paid"),
    (3, "food", 15000, "paid"),
    (4, "food", 5000, "cancelled"),
    (5, "book", 7000, "cancelled"),
]
```

이 시점의 `orders`는 일반 Python 리스트입니다. Databricks에서 만들더라도 자동으로 분산 DataFrame이 되지는 않습니다.

### 셀 2: 일반 Python으로 계산합니다

```python
totals = {}

for order_id, category, amount, status in orders:
    if status == "paid":
        totals[category] = totals.get(category, 0) + amount

python_result = sorted(totals.items())
print(python_result)
```

예상 출력은 다음과 같습니다.

```text
[('book', 20000), ('food', 15000)]
```

Python 프로세스가 각 주문을 순회하고 딕셔너리의 합계를 갱신합니다. 반복문이 끝나면 `totals`에 계산 결과가 들어 있습니다.

### 셀 3: PySpark로 같은 계산을 표현합니다

```python
from pyspark.sql import functions as F

orders_df = spark.createDataFrame(
    orders,
    schema="order_id INT, category STRING, amount LONG, status STRING",
)

paid_orders_df = orders_df.filter(F.col("status") == "paid")

sales_df = (
    paid_orders_df
    .groupBy("category")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("category")
)

sales_df.show()
```

예상 출력은 다음과 같습니다.

```text
+--------+------------+
|category|total_amount|
+--------+------------+
|    book|       20000|
|    food|       15000|
+--------+------------+
```

이 코드에서는 각 주문을 순회하는 Python 반복문을 작성하지 않습니다. 필터 조건과 그룹별 합계라는 작업을 표현하면 Spark가 실행 방법을 결정합니다.

- `schema`는 열 이름과 자료형을 명시합니다.
- `F.col("amount")`는 Python 숫자 하나가 아니라 Spark의 열을 나타내는 표현식입니다.
- `F.sum("amount")`는 Spark의 집계 함수이며 Python 내장 `sum()`과 역할이 다릅니다.
- `orderBy("category")`는 출력 비교를 위한 정렬입니다. Spark DataFrame의 행 순서는 명시적으로 정렬하지 않으면 보장되지 않습니다.

사용한 메서드는 [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)에서 확인할 수 있습니다.

> 작은 리스트로 DataFrame을 만드는 것은 학습용입니다. 실제 대규모 데이터는 먼저 Python 리스트에 전부 담지 않고, `spark.table(...)`이나 `spark.read...`로 테이블 또는 파일에서 읽는 방식으로 처리합니다.

### 셀 4: 두 결과가 같은지 확인합니다

```python
# 이 예제의 집계 결과는 두 행뿐이므로 collect()로 가져와 비교합니다.
spark_result = [
    (row["category"], row["total_amount"])
    for row in sales_df.collect()
]

assert python_result == spark_result
print("두 방식의 집계 결과가 같습니다.")
```

`collect()` 이후의 리스트 순회는 다시 일반 Python 처리입니다. `collect()`는 결과 전체를 Python 쪽으로 가져오므로 결과가 충분히 작을 때만 사용합니다. Spark Classic에서는 Driver 메모리에, Spark Connect에서는 결과를 받는 클라이언트 메모리에 적재되는 크기를 고려해야 합니다. 전체 결과를 수집한다는 동작은 [collect 공식 문서](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.collect.html)를 참고합니다.

## 4. PySpark에서는 변환과 액션을 구분합니다

**지연 실행(Lazy Evaluation)**은 DataFrame의 변환 결과를 바로 모두 계산하지 않고, 필요한 결과를 요청할 때 실행하는 방식입니다. Spark는 그전에 여러 변환을 함께 검토하여 실행 계획을 최적화할 수 있습니다.

| 구분 | 역할 | 예제 |
| --- | --- | --- |
| 변환(Transformation) | 데이터 처리 계획을 구성합니다. | `filter()`, `select()`, `withColumn()`, `groupBy().agg()`, `orderBy()` 등을 사용합니다. |
| 액션(Action) | 결과를 조회하거나 저장하기 위해 실행을 요청합니다. | `show()`, `count()`, `collect()`, `write.saveAsTable()` 등을 사용합니다. |

셀 3에서는 `sales_df`에 집계된 Python 리스트가 들어 있는 것이 아닙니다. `sales_df`는 DataFrame 객체이며, `show()`가 필요한 계산과 결과 출력을 요청합니다. 셀 4의 `collect()`도 별도의 액션입니다. 같은 DataFrame에 액션을 반복하면 캐시 등의 조건에 따라 계산이 다시 수행될 수 있습니다.

단, “액션 이전에는 아무 일도 일어나지 않는다”는 뜻은 아닙니다. 데이터 소스 확인, 스키마 추론, 계획 분석 등은 앞서 수행될 수 있습니다. 지연 실행의 기본 동작은 [PySpark DataFrame 빠른 시작](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)을 참고합니다.

## 5. Databricks에서 처음 사용할 때 주의할 점입니다

### 일반 Python 반복문은 자동으로 분산되지 않습니다

`for` 문이나 일반 Python 함수가 노트북에 있다는 이유만으로 여러 Executor에서 실행되지는 않습니다. 반면 Spark의 Python UDF처럼 명시적으로 분산 실행에 참여하는 기능도 있습니다. 입문 단계에서는 열 계산과 집계에 DataFrame API 및 `pyspark.sql.functions`의 내장 함수를 우선 사용합니다.

### 큰 DataFrame을 모두 가져오지 않습니다

`collect()`와 `toPandas()`는 전체 결과를 가져옵니다. 분산 환경에서 처리할 수 있었던 데이터라도 한 프로세스의 메모리에 모으면 메모리 부족이 발생할 수 있습니다. 내용을 확인할 때는 `show(20)`이나 `limit(20).toPandas()`처럼 가져올 행 수를 제한합니다. 이러한 제한이 항상 전체 데이터 스캔이나 집계 비용까지 줄이는 것은 아닙니다. `toPandas()`의 메모리 주의 사항은 [PySpark DataFrame 빠른 시작](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)을 참고합니다.

### PySpark가 항상 더 빠른 것은 아닙니다

이번 예제의 다섯 행처럼 작은 데이터는 일반 Python으로 처리하는 편이 간단하며 더 빠를 수도 있습니다. Spark에는 작업 계획, 스케줄링, 데이터 전송 등의 비용이 있습니다. 반면 데이터가 커지고 여러 자원을 활용할 수 있는 작업에서는 분산 처리가 유리할 수 있습니다.

특히 그룹별 집계나 조인은 같은 키의 데이터를 모으는 **셔플(Shuffle)**을 유발할 수 있습니다. 이때 실행 자원 사이의 데이터 이동이 성능에 영향을 줍니다. 컴퓨터 수만 늘린다고 모든 작업이 비례하여 빨라지지는 않습니다. 파티션과 조인 전략 등은 [Spark 성능 조정 문서](https://spark.apache.org/docs/latest/sql-performance-tuning.html)를 참고합니다.

### DataFrame 변환은 반환값을 사용합니다

`orders_df.filter(...)`를 호출했다고 해서 기존 `orders_df`가 필터링된 내용으로 바뀌지는 않습니다. 실습의 `paid_orders_df`처럼 반환된 DataFrame을 변수에 담거나 다음 연산으로 연결합니다. DataFrame 변환 메서드의 반환 형식은 [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)를 참고합니다.

## 6. 어떤 방식을 선택할지 판단합니다

다음은 입문자를 위한 판단 기준입니다. 특정 파일 크기만으로 도구를 결정하지 않고 메모리, 연산 종류, 실행 시간, 운영 환경을 함께 고려합니다.

| 상황 | 우선 고려할 방식 |
| --- | --- |
| API 호출, 파일 이름 변경, 업무 자동화 등을 구현합니다. | Python 표준 라이브러리나 목적에 맞는 라이브러리를 사용합니다. |
| 한 컴퓨터에서 충분히 처리할 수 있는 데이터를 탐색합니다. | Python 또는 pandas 등을 고려합니다. |
| 대규모 테이블을 반복적으로 정제·집계·조인합니다. | PySpark DataFrame이나 Spark SQL을 고려합니다. |
| Databricks에서 데이터 처리 흐름을 구성합니다. | Python으로 제어 로직을 작성하고 PySpark로 데이터 변환을 표현할 수 있습니다. |

## 7. 이해도를 확인합니다

1. PySpark를 사용하면 Python 문법을 새로 배워야 하는지 설명합니다.
2. `sales_df`를 만드는 코드와 `sales_df.show()`의 역할 차이를 설명합니다.
3. Databricks 노트북의 Python 반복문이 자동으로 분산되는지 설명합니다.
4. 큰 DataFrame에 무조건 `collect()`를 호출하면 안 되는 이유를 설명합니다.
5. 셀 1에 `(6, "food", 3000, "paid")`를 추가하고 셀 2부터 다시 실행하여 결과를 예측합니다.

<details>
<summary>정답과 해설을 확인합니다.</summary>

1. 같은 Python 문법을 사용합니다. 추가로 PySpark API와 Spark의 실행 방식을 학습합니다.
2. 앞부분은 필터·집계·정렬 계획을 구성하며, `show()`는 결과를 표시하는 액션입니다.
3. 자동으로 분산되지 않습니다. Spark API로 표현한 작업과 일반 Python 코드를 구분합니다.
4. 전체 결과가 한쪽 메모리에 모이므로 메모리 부족이 발생할 수 있습니다.
5. `book`은 20000을 유지하고 `food`는 18000이 됩니다. 두 방식의 결과가 여전히 같아야 합니다.

</details>

[교육 목차로 돌아갑니다.](../README.md)
