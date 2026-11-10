# Python과 PySpark의 차이 이해하기

이 문서는 Databricks를 처음 접하는 개발자를 위한 입문 자료입니다. Python의 변수, 리스트, 반복문, 함수에 대한 기초 지식을 전제로 합니다. **순수 Python → Python + pandas → PySpark** 순서로 같은 문제를 해결합니다. 기본 설명과 실습에는 약 40분이 필요하며, MySQL 확장 실습에는 환경 준비를 제외하고 약 30분이 추가로 필요합니다.

## 학습 목표

이 문서를 학습하면 다음 내용을 설명할 수 있습니다.

- Python, Apache Spark, PySpark, Databricks의 관계를 설명합니다.
- 순수 Python, Python + pandas, PySpark 코드의 실행 방식 차이를 구분합니다.
- 같은 데이터를 세 방식으로 필터링하고 집계합니다.
- 지연 실행과 분산 환경에서 발생하기 쉬운 실수를 이해합니다.
- MySQL JDBC 읽기부터 집계 결과 조회까지 Driver와 Executor의 역할을 구분합니다.

## 1. Python은 언어이고, PySpark는 Python으로 Spark를 사용하는 API입니다

**PySpark는 Python을 대체하는 별도의 프로그래밍 언어가 아닙니다.** PySpark 코드도 Python 문법으로 작성합니다. 차이는 데이터 처리 작업을 어떤 도구에 맡기고, 어디에서 실행하는지에 있습니다.

| 이름 | 의미 | 이 교육에서의 역할 |
| --- | --- | --- |
| Python | 범용 프로그래밍 언어입니다. | 변수, 조건문, 반복문, 함수 등 프로그램을 작성합니다. |
| pandas | Python의 표 형태 데이터 처리 라이브러리입니다. | 한 컴퓨터에서 DataFrame을 필터링하고 집계합니다. |
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

| 비교 항목 | 순수 Python | Python + pandas | PySpark |
| --- | --- | --- | --- |
| 개발자가 작성하는 내용 | 각 항목을 어떻게 처리할지 작성합니다. | 열 선택과 그룹별 집계를 pandas 연산으로 표현합니다. | 어떤 열을 선택하고 어떤 조건으로 집계할지 Spark 연산으로 표현합니다. |
| 주요 데이터 구조 | 리스트, 딕셔너리 등을 사용합니다. | pandas DataFrame을 사용합니다. | Spark DataFrame을 사용합니다. |
| 실행 주체 | 예제 코드를 실행하는 Python 프로세스가 계산합니다. | 해당 Python 프로세스에서 pandas와 내부 연산 라이브러리가 계산합니다. | Spark가 계획을 최적화하고 작업을 배분합니다. |
| 계산 시점 | 예제의 반복문을 실행할 때 계산합니다. | 예제의 필터와 `agg()`를 실행할 때 결과를 계산합니다. | 변환을 계획하고 액션을 호출할 때 필요한 계산을 수행합니다. |
| 데이터 규모 | 리스트에 담은 데이터는 해당 프로세스의 메모리를 사용합니다. | 예제는 읽은 데이터와 중간 결과를 한 컴퓨터의 메모리에 담습니다. | 파티션별로 나누어 여러 실행 자원을 활용할 수 있습니다. |
| 성능 고려 사항 | 알고리즘, 메모리 사용량 등을 고려합니다. | 자료형, 열 단위 연산, 중간 결과의 크기 등을 고려합니다. | 실행 계획, 파티션, 데이터 이동, 자원 사용량도 고려합니다. |

표의 실행 시점 비교는 아래 예제를 기준으로 합니다. Python에도 제너레이터처럼 계산을 미루는 기능이 있으므로, “Python은 항상 즉시 실행하고 PySpark는 항상 모든 것을 미룬다”라고 일반화하지 않습니다.

### pandas와 PySpark DataFrame도 구분해야 합니다

pandas는 Python에서 표 형태의 데이터를 다루는 라이브러리입니다. 일반적인 pandas DataFrame은 한 컴퓨터의 메모리를 중심으로 처리합니다. PySpark DataFrame은 분산 처리할 수 있는 데이터와 처리 계획을 표현합니다.

두 객체는 이름에 `DataFrame`이 들어가지만 메서드와 실행 방식이 다릅니다. 또한 `pyspark.pandas`로 사용하는 **pandas API on Spark**는 일반 pandas와 구분해야 합니다. 이 문서는 `import pandas as pd`로 사용하는 일반 pandas와 `pyspark.sql`의 DataFrame API를 비교합니다. API별 역할은 [PySpark 공식 개요](https://spark.apache.org/docs/latest/api/python/)에서 확인할 수 있습니다.

## 3. 순수 Python → Python + pandas → PySpark로 같은 문제를 해결합니다

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

순수 Python 예제에는 추가 패키지가 필요하지 않습니다. pandas 예제에는 `pandas` 패키지가 필요하며, 로컬에서 실행한다면 사용하는 Python 환경에 설치합니다.

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

### 셀 2: 순수 Python으로 계산합니다

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

### 셀 3: Python + pandas로 계산합니다

```python
import pandas as pd

orders_pdf = pd.DataFrame(
    orders,
    columns=["order_id", "category", "amount", "status"],
)

paid_orders_pdf = orders_pdf.loc[orders_pdf["status"] == "paid"]
sales_pdf = (
    paid_orders_pdf
    .groupby("category", as_index=False)
    .agg(total_amount=("amount", "sum"))
    .sort_values("category")
)

pandas_result = list(sales_pdf.itertuples(index=False, name=None))
print(pandas_result)
```

예상 출력은 다음과 같습니다.

```text
[('book', 20000), ('food', 15000)]
```

`loc`으로 결제 완료 행을 선택하고 `groupby().agg()`로 합계를 계산합니다. `as_index=False`는 그룹 키인 `category`를 결과의 일반 열로 유지합니다. `sort_values()`로 순서를 맞추고 `itertuples()`로 비교하기 쉬운 튜플 목록을 만듭니다. `pdf`는 이 문서에서 pandas DataFrame을 구분하기 위해 붙인 변수명입니다.

반복문을 직접 작성하지 않아도 이 예제의 계산은 한 컴퓨터에서 수행합니다. `agg()`가 끝나면 `sales_pdf`에 집계 결과가 있으며, `print()`는 이미 계산한 결과를 표시합니다. pandas의 `groupby()` 자체는 그룹 처리 객체를 만들지만, Spark처럼 여러 변환 전체를 Action까지 모아 분산 실행하는 방식은 아닙니다. 집계 문법은 [pandas GroupBy 문서](https://pandas.pydata.org/docs/user_guide/groupby.html)를 참고합니다.

### 셀 4: PySpark로 같은 계산을 표현합니다

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

### 셀 5: 세 결과가 같은지 확인합니다

```python
# 이 예제의 집계 결과는 두 행뿐이므로 collect()로 가져와 비교합니다.
spark_result = [
    (row["category"], row["total_amount"])
    for row in sales_df.collect()
]

assert python_result == pandas_result == spark_result
print("세 방식의 집계 결과가 같습니다.")
```

`collect()` 이후의 리스트 순회는 다시 일반 Python 처리입니다. `collect()`는 결과 전체를 Python 쪽으로 가져오므로 결과가 충분히 작을 때만 사용합니다. Spark Classic에서는 Driver 메모리에, Spark Connect에서는 결과를 받는 클라이언트 메모리에 적재되는 크기를 고려해야 합니다. 전체 결과를 수집한다는 동작은 [collect 공식 문서](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.collect.html)를 참고합니다.

## 4. PySpark에서는 변환과 액션을 구분합니다

**지연 실행(Lazy Evaluation)**은 DataFrame의 변환 결과를 바로 모두 계산하지 않고, 필요한 결과를 요청할 때 실행하는 방식입니다. Spark는 그전에 여러 변환을 함께 검토하여 실행 계획을 최적화할 수 있습니다.

| 구분 | 역할 | 예제 |
| --- | --- | --- |
| 변환(Transformation) | 데이터 처리 계획을 구성합니다. | `filter()`, `select()`, `withColumn()`, `groupBy().agg()`, `orderBy()` 등을 사용합니다. |
| 액션(Action) | 결과를 조회하거나 저장하기 위해 실행을 요청합니다. | `show()`, `count()`, `collect()`, `write.saveAsTable()` 등을 사용합니다. |

셀 4에서는 `sales_df`에 집계된 Python 리스트가 들어 있는 것이 아닙니다. `sales_df`는 DataFrame 객체이며, `show()`가 필요한 계산과 결과 출력을 요청합니다. 셀 5의 `collect()`도 별도의 액션입니다. 같은 DataFrame에 액션을 반복하면 캐시 등의 조건에 따라 계산이 다시 수행될 수 있습니다.

단, “액션 이전에는 아무 일도 일어나지 않는다”는 뜻은 아닙니다. 데이터 소스 확인, 스키마 추론, 계획 분석 등은 앞서 수행될 수 있습니다. 지연 실행의 기본 동작은 [PySpark DataFrame 빠른 시작](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)을 참고합니다.

## 5. MySQL 데이터도 순수 Python → Python + pandas → PySpark로 집계합니다

앞에서는 코드에 정의한 주문 데이터를 사용했습니다. 이번에는 **같은 MySQL 테이블에서 주문을 읽어 결제 완료 주문의 카테고리별 매출과 주문 수를 집계**합니다. 세 예제 모두 필터와 집계를 Python, pandas 또는 Spark에서 수행하도록 작성합니다. SQL에는 `WHERE`나 `GROUP BY`를 넣지 않아 처리 주체의 차이를 비교합니다.

이 절의 “순수 Python”은 **필터와 집계를 pandas나 Spark 없이 반복문과 딕셔너리로 구현한다는 뜻**입니다. MySQL 연결에는 별도 라이브러리인 PyMySQL을 사용합니다. pandas 예제는 SQLAlchemy와 PyMySQL로 연결하고, PySpark 예제는 Java 기반 데이터베이스 연결 인터페이스인 JDBC를 사용합니다.

### 5.1. 실습 환경과 MySQL 테이블을 준비합니다

세 예제는 MySQL에 연결할 수 있는 **Databricks 클래식 컴퓨팅**과 Python 노트북을 전제로 합니다. Serverless나 SQL Warehouse의 연결 방식과 설정에 그대로 적용하는 예제는 아닙니다. 앞의 리스트 실습과 달리 다음 준비가 필요합니다.

| 준비 항목 | 필요한 내용 |
| --- | --- |
| 네트워크 | Driver와 Executor가 실행되는 컴퓨팅에서 MySQL 호스트와 포트에 접근할 수 있어야 합니다. |
| Python 라이브러리 | 순수 Python 예제에는 `PyMySQL`이 필요하며, pandas 예제에는 추가로 `pandas`와 `SQLAlchemy`가 필요합니다. 누락된 패키지는 노트북의 `%pip install`로 설치합니다. |
| JDBC 라이브러리 | MySQL과 Runtime의 Java 버전에 맞는 MySQL Connector/J를 컴퓨팅 라이브러리로 준비합니다. Driver와 Executor 양쪽에서 사용할 수 있어야 합니다. |
| MySQL 계정 | 실습 테이블을 읽을 수 있는 `SELECT` 권한을 준비합니다. 테이블 생성은 별도의 준비 작업으로 수행합니다. |
| 비밀 값 | Databricks의 `mysql-training` Secret Scope에 `username`, `password` 키를 등록하고 읽기 권한을 준비합니다. |
| TLS | Python의 SSLContext와 JDBC의 `VERIFY_IDENTITY`가 서버 인증서를 검증할 수 있도록 호스트 이름과 인증서 신뢰 설정을 준비합니다. Python과 Java의 인증서 신뢰 저장소는 별개입니다. |

Connector/J가 없다면 컴퓨팅의 라이브러리 설정에서 호환되는 JAR 또는 Maven 패키지를 설치합니다. Python용 MySQL 패키지를 `%pip`로 설치하는 것과는 다릅니다. 설정은 [Databricks 컴퓨팅 라이브러리](https://docs.databricks.com/aws/en/libraries/cluster-libraries), [Secret 관리](https://docs.databricks.com/aws/en/security/secrets), [MySQL TLS 연결](https://dev.mysql.com/doc/connector-j/en/connector-j-reference-using-ssl.html)을 참고합니다.

다음 SQL은 **MySQL 클라이언트에서** 실행합니다. Databricks의 Spark SQL 셀에서 실행하는 코드가 아닙니다. 아래 예제는 `education` 데이터베이스가 준비되어 있다고 가정하며, 새 실습 테이블에 한 번만 실행합니다. 앞의 확인 문제에서 추가한 주문은 포함하지 않습니다.

```sql
CREATE TABLE education.orders_pyspark_demo (
    order_id BIGINT PRIMARY KEY,
    category VARCHAR(30) NOT NULL,
    amount BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL
);

INSERT INTO education.orders_pyspark_demo
    (order_id, category, amount, status)
VALUES
    (1, 'book', 12000, 'paid'),
    (2, 'book', 8000, 'paid'),
    (3, 'food', 15000, 'paid'),
    (4, 'food', 5000, 'cancelled'),
    (5, 'book', 7000, 'cancelled');
```

다음 공통 설정은 세 예제보다 먼저 Databricks Python 노트북에서 실행합니다. `mysql.example.com`을 실제 MySQL 호스트로 변경합니다. 인증 정보는 Secret에서 읽습니다. Python은 기본 신뢰 저장소를 사용하며, 사설 CA를 사용한다면 `ssl.create_default_context(cafile="CA 파일 경로")`로 변경합니다. JDBC용 CA 신뢰 설정은 별도로 준비합니다.

```python
import ssl

mysql_host = "mysql.example.com"
mysql_port = 3306
mysql_database = "education"
mysql_user = dbutils.secrets.get(scope="mysql-training", key="username")
mysql_password = dbutils.secrets.get(scope="mysql-training", key="password")
mysql_tls = ssl.create_default_context()

orders_query = """
SELECT order_id, category, amount, status
FROM orders_pyspark_demo
"""
```

### 5.2. 순수 Python으로 읽고 집계합니다

```python
import pymysql

mysql_totals = {}

with pymysql.connect(
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database,
    ssl=mysql_tls,
    cursorclass=pymysql.cursors.SSCursor,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute(orders_query)
        for order_id, category, amount, status in cursor:
            if status == "paid":
                summary = mysql_totals.setdefault(
                    category, {"total_amount": 0, "order_count": 0}
                )
                summary["total_amount"] += amount
                summary["order_count"] += 1

mysql_python_result = [
    (category, summary["total_amount"], summary["order_count"])
    for category, summary in sorted(mysql_totals.items())
]
print(mysql_python_result)
```

예상 출력은 다음과 같습니다.

```text
[('book', 20000, 2), ('food', 15000, 1)]
```

Python 프로세스가 MySQL에 연결하고 행을 순회하며 합계와 건수를 갱신합니다. `SSCursor`는 전체 조회 결과를 한꺼번에 버퍼에 담지 않고 읽는 커서이며, 이 예제는 카테고리별 집계 상태를 딕셔너리에 보관합니다. 입력 전체를 리스트로 저장하지 않더라도 카테고리 수가 많아지면 집계 상태의 메모리는 증가합니다. 연결과 커서는 `with` 블록을 벗어날 때 닫힙니다. 연결 옵션과 커서 동작은 [PyMySQL Connection](https://pymysql.readthedocs.io/en/latest/modules/connections.html) 및 [SSCursor 문서](https://pymysql.readthedocs.io/en/latest/modules/cursors.html#pymysql.cursors.SSCursor)를 참고합니다.

### 5.3. Python + pandas로 읽고 집계합니다

```python
import pandas as pd
from sqlalchemy import URL, create_engine, text

mysql_url = URL.create(
    "mysql+pymysql",
    username=mysql_user,
    password=mysql_password,
    host=mysql_host,
    port=mysql_port,
    database=mysql_database,
)
engine = create_engine(mysql_url, connect_args={"ssl": mysql_tls})

try:
    with engine.connect() as connection:
        mysql_orders_pdf = pd.read_sql_query(text(orders_query), connection)
finally:
    engine.dispose()

mysql_paid_pdf = mysql_orders_pdf.loc[mysql_orders_pdf["status"] == "paid"]
mysql_sales_pdf = (
    mysql_paid_pdf
    .groupby("category", as_index=False)
    .agg(
        total_amount=("amount", "sum"),
        order_count=("order_id", "size"),
    )
    .sort_values("category")
)

mysql_pandas_result = list(mysql_sales_pdf.itertuples(index=False, name=None))
print(mysql_pandas_result)
assert mysql_python_result == mysql_pandas_result
```

예상 출력은 다음과 같습니다.

```text
[('book', 20000, 2), ('food', 15000, 1)]
```

`read_sql_query()`는 이 예제처럼 `chunksize`를 지정하지 않으면 조회 결과 전체를 pandas DataFrame으로 읽습니다. 이후 필터와 집계는 Python 프로세스 안에서 수행하며 Spark Executor에 작업을 나누지 않습니다. `size`는 그룹의 행 수를 세므로 Spark의 `F.count("*")`와 비교하기 적합합니다. `print()`를 호출해야 집계가 시작되는 것은 아닙니다.

MySQL 연결은 pandas가 지원하는 SQLAlchemy 연결을 사용합니다. `URL.create()`는 비밀번호의 특수문자를 직접 URL 문자열로 조합할 필요 없이 전달하며, `dispose()`는 사용이 끝난 연결 풀을 정리합니다. 관련 API는 [pandas.read_sql_query](https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html)와 [SQLAlchemy Engine 설정](https://docs.sqlalchemy.org/en/20/core/engines.html)을 참고합니다.

### 5.4. PySpark로 읽고 집계합니다

#### 5.4.1. JDBC 데이터 소스를 정의합니다

다음 코드는 5.1절의 공통 연결 정보를 사용합니다. 앞의 두 방식과 같은 테이블을 읽지만, 실제 행 읽기는 Executor의 JDBC 작업으로 수행합니다.

```python
from pyspark.sql import functions as F

jdbc_url = (
    f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
    "?sslMode=VERIFY_IDENTITY"
)

mysql_orders_df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "orders_pyspark_demo")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("user", mysql_user)
    .option("password", mysql_password)
    .option("partitionColumn", "order_id")
    .option("lowerBound", "1")
    .option("upperBound", "6")
    .option("numPartitions", "2")
    # 학습용: 필터와 집계를 Spark에서 관찰하도록 푸시다운을 끕니다.
    .option("pushDownPredicate", "false")
    .option("pushDownAggregate", "false")
    .load()
)
```

여기서 `.load()`는 JDBC 소스를 나타내는 DataFrame을 생성합니다. `show()`처럼 결과 행을 요청하는 Action과 구분합니다. **다만 스키마를 확인하기 위한 MySQL 연결과 조회는 이 단계에서 발생할 수 있습니다.** “아직 Action을 호출하지 않았으므로 MySQL에는 전혀 연결하지 않는다”라고 이해하면 안 됩니다. JDBC 읽기와 스키마 확인은 [Databricks JDBC 예제](https://docs.databricks.com/aws/en/archive/connectors/jdbc)를 참고합니다.

`driver` 옵션의 `com.mysql.cj.jdbc.Driver`는 **MySQL에 연결하는 JDBC 드라이버 클래스 이름**입니다. Spark의 작업을 조율하는 **Spark Driver 프로세스**와는 다른 의미입니다. 클래스 이름은 [MySQL Connector/J 문서](https://dev.mysql.com/doc/connector-j/en/connector-j-usagenotes-connect-drivermanager.html)에서 확인할 수 있습니다.

읽기 옵션은 다음처럼 해석합니다.

| 옵션 | 이 예제에서의 의미 |
| --- | --- |
| `partitionColumn="order_id"` | 숫자형 주문 번호를 기준으로 JDBC 읽기 범위를 나눕니다. MySQL 테이블 자체를 물리적으로 분할하지는 않습니다. |
| `lowerBound="1"`, `upperBound="6"` | 읽기 파티션의 범위 간격을 계산하는 기준입니다. `1 <= order_id < 6`으로 행을 필터링하는 조건은 아닙니다. |
| `numPartitions="2"` | 이 읽기에서 사용할 최대 파티션 수를 지정합니다. 충분한 실행 슬롯이 있으면 두 읽기 작업을 병렬로 수행할 수 있습니다. Executor 두 개를 생성하는 설정은 아닙니다. |
| `pushDownPredicate="false"` | `status == "paid"` 같은 필터를 MySQL에 위임하지 않고 Spark에서 처리하도록 합니다. 읽기 파티션을 구분하는 SQL 조건은 여전히 사용합니다. |
| `pushDownAggregate="false"` | JDBC V2 경로에서 집계를 MySQL에 위임하지 않도록 합니다. JDBC V1 경로는 이 옵션 없이도 Spark에서 집계합니다. |

푸시다운(Pushdown)은 연산을 원본 데이터베이스에서 수행하도록 전달하는 최적화입니다. 위 설정은 실행 주체를 학습하기 위한 것이며, 운영에서는 DB 부하와 전송량을 보고 판단합니다. 옵션의 적용 범위는 [Spark JDBC 데이터 소스 문서](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)를 참고합니다.

#### 5.4.2. Transformation으로 필터와 집계 계획을 구성합니다

```python
mysql_paid_df = (
    mysql_orders_df
    .filter(F.col("status") == "paid")
    .select("category", "amount")
)

mysql_sales_df = (
    mysql_paid_df
    .groupBy("category")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("order_count"),
    )
    .orderBy("category")
)

# 결과 행을 읽는 대신 실행 계획을 확인합니다.
mysql_sales_df.explain(mode="formatted")
```

`filter()`, `select()`, `groupBy().agg()`, `orderBy()`는 Transformation입니다. `F.count("*")`도 여기서는 집계 표현식을 만들며, 이 줄만으로 주문 수를 계산하지 않습니다. 반면 DataFrame의 `count()` 메서드는 행 수를 반환하는 Action입니다.

`explain()`은 실행 계획을 출력합니다. 결과 행을 조회하는 Action은 아니지만 계획 분석이나 메타데이터 접근은 일어날 수 있습니다. 출력에서 JDBC 스캔, 필터, 집계, `Exchange`를 찾아봅니다. `Exchange`는 파티션 사이의 데이터 재분배 지점을 나타냅니다. 실제 연산 이름과 배치는 Runtime 및 최적화에 따라 달라집니다. 출력 형식은 [DataFrame.explain 공식 문서](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)를 참고합니다.

#### 5.4.3. Action으로 읽기와 집계를 실행합니다

```python
# Action: 필요한 JDBC 읽기와 집계를 실행하고 결과를 표시합니다.
mysql_sales_df.show(truncate=False)
```

테이블에 위의 다섯 행만 있다면 예상 결과는 다음과 같습니다.

```text
+--------+------------+-----------+
|category|total_amount|order_count|
+--------+------------+-----------+
|book    |20000       |2          |
|food    |15000       |1          |
+--------+------------+-----------+
```

`show()`가 표시하는 행 수는 제한되지만, 이 예제의 카테고리별 합계를 구하려면 입력 전체를 확인해야 합니다. 또한 뒤이어 `mysql_sales_df.count()`를 실행하면 반환값은 주문 수 3이 아니라 **집계 결과의 행 수인 2**입니다. 캐시 없이 Action을 반복하면 MySQL을 다시 읽을 수 있으므로, 실행 횟수와 데이터베이스 부하도 함께 확인합니다.

세 방식의 실행 차이는 다음과 같습니다.

| 비교 항목 | 순수 Python | Python + pandas | PySpark |
| --- | --- | --- | --- |
| MySQL 연결 | Python 프로세스에서 PyMySQL로 연결합니다. | Python 프로세스에서 SQLAlchemy와 PyMySQL로 연결합니다. | Executor의 읽기 Task가 JDBC로 연결합니다. Driver도 스키마 확인을 위해 연결할 수 있습니다. |
| 입력 처리 | 커서를 순회하며 행별로 처리합니다. | 예제에서는 조회 결과 전체를 pandas DataFrame으로 읽습니다. | JDBC 파티션별로 읽습니다. |
| 필터와 집계 | Python 반복문과 딕셔너리로 계산합니다. | pandas의 필터와 `groupby().agg()`로 계산합니다. | Spark의 Transformation으로 표현하고 Executor에서 계산합니다. |
| 결과 계산 시점 | 커서를 순회할 때 계산합니다. | 필터와 `agg()`를 실행할 때 계산합니다. | `show()` 같은 Action이 실행을 요청합니다. |

이 절의 클래식 컴퓨팅에서는 앞의 두 예제가 노트북의 Python 프로세스가 있는 Driver 측 자원을 사용합니다. pandas DataFrame으로 바꾸었다고 해서 Executor를 활용하는 것은 아닙니다. 세 예제는 각각 MySQL을 조회하므로 결과를 비교하는 동안 실습 테이블을 변경하지 않습니다.

### 5.5. Driver와 Executor가 맡는 역할을 확인합니다

![MySQL JDBC 집계 흐름입니다. Driver가 읽기 및 집계 계획을 만들고 Action 호출 후 작업을 배분합니다. Executor는 MySQL에서 파티션별 데이터를 직접 읽고 부분 집계한 뒤, 셔플과 최종 집계를 수행합니다. 표시할 결과는 Driver를 거쳐 노트북으로 전달됩니다.](images/mysql-jdbc-driver-executor.png)

그림은 위 코드의 개념적인 실행 흐름입니다. 두 Executor를 그렸지만, 실제로는 하나의 Executor가 여러 태스크를 실행할 수 있습니다. 셔플 이후의 파티션 수와 실행 태스크 수 역시 JDBC 읽기의 `numPartitions`와 일치할 필요가 없습니다.

| 단계 | Driver가 수행하는 일 | Executor와 MySQL이 수행하는 일 |
| --- | --- | --- |
| ① 소스와 계획을 준비합니다. | JDBC 연결 정보와 스키마를 확인하고 필터·집계 계획을 구성합니다. | MySQL이 스키마 조회에 응답할 수 있습니다. 아직 집계용 전체 행을 읽는 단계는 아닙니다. |
| ② `show()`를 호출합니다. | 실행 계획을 최적화하고 Job과 Stage의 실행을 조율하며 Task를 배분합니다. | Executor가 할당된 Task를 실행합니다. Action 하나가 반드시 Job 하나에만 대응하는 것은 아닙니다. |
| ③ JDBC로 읽습니다. | 진행 상황을 관리합니다. | Executor의 읽기 Task가 JDBC 연결을 열고 범위별 SQL 조회를 요청합니다. MySQL이 해당 조회를 수행하여 행을 각 Executor에 직접 반환합니다. |
| ④ 부분 집계합니다. | 다음 단계 실행을 조율합니다. | Executor가 결제 완료 행을 골라 파티션 안에서 카테고리별 부분 합계와 건수를 계산합니다. |
| ⑤ 셔플하고 최종 집계합니다. | 단계 간 의존성을 관리합니다. | 같은 카테고리의 부분 결과를 모아 최종 합계와 건수를 계산합니다. 이 계산도 Executor에서 수행합니다. |
| ⑥ 결과를 표시합니다. | 표시할 결과를 받아 노트북에 전달합니다. | Executor가 정렬 등 남은 연산을 처리하고 표시할 결과를 반환합니다. |

**원본 주문 전체를 Driver에 모은 뒤 Executor에 나누어 주는 흐름이 아닙니다.** 또한 Driver가 부분 합계를 모두 받아 최종 집계하는 것도 아닙니다. Driver는 실행을 조율하고, 이 예제의 데이터 읽기와 집계는 Executor의 작업으로 수행합니다. 역할 구분은 [Spark 클러스터 개요](https://spark.apache.org/docs/latest/cluster-overview.html)를 참고합니다.

그림의 셔플은 특정 서버 한 대를 뜻하지 않습니다. Executor의 태스크들이 파티션별 중간 결과를 교환하는 과정입니다. 실제 계획에서는 정렬을 위한 추가 셔플이 생기거나 적응형 실행(AQE)이 파티션을 합칠 수 있습니다. Spark UI에서 실제 Job, Stage, Task와 실행 계획을 함께 확인합니다.

### 5.6. 병렬 읽기와 반복 실행 시 주의합니다

- `numPartitions`를 크게 설정하면 MySQL에 대한 동시 조회 부하도 커질 수 있으므로 작은 값부터 확인합니다. 이 값은 다른 작업과 스키마 조회를 포함한 MySQL 전체 연결 수의 상한은 아닙니다.
- 이 예제는 읽는 동안 데이터가 바뀌지 않는 실습 테이블을 가정합니다. 여러 JDBC 연결로 읽는 데이터가 자동으로 동일한 시점의 스냅샷이 되는 것은 아닙니다.
- 필요한 데이터 범위는 실제 필터 조건으로 지정합니다. `lowerBound`와 `upperBound`만으로 조회 범위를 제한하지 않습니다.
- `cache()`도 즉시 MySQL을 읽는 Action은 아닙니다. 후속 Action이 실행되며 캐시가 채워지고, 캐시되지 않았거나 유실된 부분은 다시 읽을 수 있습니다.

## 6. Databricks에서 처음 사용할 때 주의할 점입니다

### 일반 Python 반복문은 자동으로 분산되지 않습니다

`for` 문이나 일반 Python 함수가 노트북에 있다는 이유만으로 여러 Executor에서 실행되지는 않습니다. 반면 Spark의 Python UDF처럼 명시적으로 분산 실행에 참여하는 기능도 있습니다. 입문 단계에서는 열 계산과 집계에 DataFrame API 및 `pyspark.sql.functions`의 내장 함수를 우선 사용합니다.

### 큰 DataFrame을 모두 가져오지 않습니다

`collect()`와 `toPandas()`는 전체 결과를 가져옵니다. 분산 환경에서 처리할 수 있었던 데이터라도 한 프로세스의 메모리에 모으면 메모리 부족이 발생할 수 있습니다. 내용을 확인할 때는 `show(20)`이나 `limit(20).toPandas()`처럼 가져올 행 수를 제한합니다. 이러한 제한이 항상 전체 데이터 스캔이나 집계 비용까지 줄이는 것은 아닙니다. `toPandas()`의 메모리 주의 사항은 [PySpark DataFrame 빠른 시작](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)을 참고합니다.

### PySpark가 항상 더 빠른 것은 아닙니다

이번 예제의 다섯 행처럼 작은 데이터는 일반 Python으로 처리하는 편이 간단하며 더 빠를 수도 있습니다. Spark에는 작업 계획, 스케줄링, 데이터 전송 등의 비용이 있습니다. 반면 데이터가 커지고 여러 자원을 활용할 수 있는 작업에서는 분산 처리가 유리할 수 있습니다.

특히 그룹별 집계나 조인은 같은 키의 데이터를 모으는 **셔플(Shuffle)**을 유발할 수 있습니다. 이때 실행 자원 사이의 데이터 이동이 성능에 영향을 줍니다. 컴퓨터 수만 늘린다고 모든 작업이 비례하여 빨라지지는 않습니다. 파티션과 조인 전략 등은 [Spark 성능 조정 문서](https://spark.apache.org/docs/latest/sql-performance-tuning.html)를 참고합니다.

### DataFrame 변환은 반환값을 사용합니다

`orders_df.filter(...)`를 호출했다고 해서 기존 `orders_df`가 필터링된 내용으로 바뀌지는 않습니다. 실습의 `paid_orders_df`처럼 반환된 DataFrame을 변수에 담거나 다음 연산으로 연결합니다. DataFrame 변환 메서드의 반환 형식은 [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)를 참고합니다.

## 7. 어떤 방식을 선택할지 판단합니다

다음은 입문자를 위한 판단 기준입니다. 특정 파일 크기만으로 도구를 결정하지 않고 메모리, 연산 종류, 실행 시간, 운영 환경을 함께 고려합니다.

| 상황 | 우선 고려할 방식 |
| --- | --- |
| API 호출, 파일 이름 변경, 업무 자동화 등을 구현합니다. | Python 표준 라이브러리나 목적에 맞는 라이브러리를 사용합니다. |
| 한 컴퓨터에서 충분히 처리할 수 있는 데이터를 탐색합니다. | Python 또는 pandas 등을 고려합니다. |
| 대규모 테이블을 반복적으로 정제·집계·조인합니다. | PySpark DataFrame이나 Spark SQL을 고려합니다. |
| Databricks에서 데이터 처리 흐름을 구성합니다. | Python으로 제어 로직을 작성하고 PySpark로 데이터 변환을 표현할 수 있습니다. |

## 8. 이해도를 확인합니다

1. PySpark를 사용하면 Python 문법을 새로 배워야 하는지 설명합니다.
2. `sales_df`를 만드는 코드와 `sales_df.show()`의 역할 차이를 설명합니다.
3. Databricks 노트북의 Python 반복문이 자동으로 분산되는지 설명합니다.
4. 큰 DataFrame에 무조건 `collect()`를 호출하면 안 되는 이유를 설명합니다.
5. 셀 1에 `(6, "food", 3000, "paid")`를 추가하고 셀 2부터 다시 실행하여 결과를 예측합니다.
6. MySQL 예제에서 `.load()`와 `.show()`의 역할 차이를 설명합니다.
7. PySpark의 MySQL 예제에서 원본 행을 읽고 최종 집계하는 주체가 Driver인지 Executor인지 설명합니다.
8. `numPartitions=2`가 Executor 두 개를 생성하거나 최종 결과를 두 행으로 제한하는 설정인지 설명합니다.
9. pandas의 `groupby().agg()`와 PySpark의 `groupBy().agg()`가 결과를 계산하는 시점과 위치를 비교합니다.

<details>
<summary>정답과 해설을 확인합니다.</summary>

1. 같은 Python 문법을 사용합니다. 추가로 PySpark API와 Spark의 실행 방식을 학습합니다.
2. 앞부분은 필터·집계·정렬 계획을 구성하며, `show()`는 결과를 표시하는 액션입니다.
3. 자동으로 분산되지 않습니다. Spark API로 표현한 작업과 일반 Python 코드를 구분합니다.
4. 전체 결과가 한쪽 메모리에 모이므로 메모리 부족이 발생할 수 있습니다.
5. `book`은 20000을 유지하고 `food`는 18000이 됩니다. 세 방식의 결과가 여전히 같아야 합니다.
6. `.load()`는 읽을 소스를 DataFrame으로 정의하며 스키마 조회가 발생할 수 있습니다. `.show()`는 결과를 표시하기 위해 필요한 읽기와 집계를 실행하는 Action입니다.
7. PySpark 예제에서는 Executor의 Task가 MySQL에서 직접 읽고 집계합니다. Driver는 실행을 조율하고 표시할 결과를 받아 전달합니다. 순수 Python과 pandas 예제는 노트북의 Python 프로세스에서 처리합니다.
8. 둘 다 아닙니다. JDBC 읽기의 최대 파티션 수를 지정하며 Executor 수나 결과 행 수를 지정하지 않습니다.
9. pandas 예제는 `agg()` 실행 시 해당 Python 프로세스에서 결과를 계산합니다. PySpark 예제는 집계 계획을 구성한 뒤 Action이 요청되면 Executor에서 계산합니다.

</details>

[교육 목차로 돌아갑니다.](../README.md)
