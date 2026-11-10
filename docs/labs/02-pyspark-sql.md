# 실습 2. PySpark와 SQL로 같은 결과 만들기

**연결 본문:** [07. Databricks에서 실행](../course/07-databricks.md).

**준비:** Databricks Workspace와 Python/Spark 지원 노트북 컴퓨트 사용 권한.

**완료 기준:** PySpark와 SQL의 집계 결과가 모두 book=20000, food=15000입니다.

## 1. 노트북을 가져옵니다

[02_pyspark_sql.py](../../labs/02_pyspark_sql.py)를 다운로드하거나 저장소에서 찾고, Workspace의 폴더에서 **Import → 파일 선택**으로 가져옵니다. 일반 Python 코드가 아니라 셀 구분을 포함한 소스 노트북입니다. 메뉴는 환경에 따라 다를 수 있습니다. [공식 가져오기 안내](https://docs.databricks.com/aws/en/notebooks/notebook-export-import)

노트북 상단에서 허용된 Python/Spark 컴퓨트를 연결합니다. SQL 전용 실행 환경은 이 Python 실습의 대상이 아닙니다. Databricks가 제공하는 `spark` 객체를 사용하므로 별도 Spark 클러스터 설치 명령은 없습니다.

## 2. 위에서 아래로 실행합니다

| 코드 셀 | 하는 일 | 확인할 결과 |
| --- | --- | --- |
| 입력 생성 | CSV와 같은 5행·스키마 정의 | 4개 열, 5개 주문 |
| PySpark 계획 | 필터·집계·정렬과 `explain()` | 실행 계획; 환경별 출력 차이는 정상 |
| PySpark 조회 | `show()`로 실행 요청 | 카테고리별 2행 |
| SQL 조회 | `orders` 임시 뷰를 만들고 SQL 실행 | 같은 2행 |
| 비교 | 작은 결과를 수집해 정답 검사 | 결과 일치 메시지 |

```text
category  total_amount
book      20000
food      15000

PySpark·SQL 결과 일치: book=20000, food=15000
```

입력은 파일 업로드 없이 셀에서 직접 만듭니다. 현재 Python 세션에서 생성한 작은 리스트를 Spark로 전달하는 교육 예제이며, 실무의 대용량 원본은 저장소·테이블에서 직접 읽습니다.

## 3. 실행 위치를 설명합니다

`orders` 리스트는 Python에 있고, `orders_df`는 Spark DataFrame입니다. PySpark와 SQL은 같은 Spark 엔진을 통해 결과를 구합니다. 이 실습에서는 사용자 정의 Python 함수를 행마다 분산 실행하지 않습니다.

집계 결과가 두 행으로 제한된 것을 알고 있으므로 비교 셀에서 `collect()`를 사용합니다. 큰 원본을 전부 수집하는 방법으로 일반화하지 않습니다. `show()` 이후 비교할 때 다시 계산될 수도 있습니다.

## 막혔을 때

| 증상 | 확인할 것 |
| --- | --- |
| `spark`가 없음 | Databricks의 Python/Spark 노트북 컴퓨트에 연결했는지 |
| `orders_df`가 없음 | 입력 생성 셀부터 실행했는지 |
| `orders` 뷰를 찾을 수 없음 | 같은 세션에서 임시 뷰 등록 셀을 실행했는지 |
| 기대 결과와 다름 | 입력 다섯 행, `paid` 조건, 합계 열을 확인 |

실습 후 세션을 종료하면 임시 뷰를 다시 만들어야 합니다. 영구 테이블은 생성하지 않았으므로 테이블 삭제는 필요하지 않습니다. 본인이 시작한 컴퓨트의 사용 상태를 확인합니다.

[본문으로 돌아가기](../course/07-databricks.md) · [다음 개념: 08. Unity Catalog](../course/08-unity-catalog.md) · [실습 목차](README.md)
