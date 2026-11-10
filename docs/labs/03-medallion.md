# 실습 3. 관리형 Delta 테이블로 파이프라인 만들기

**연결 본문:** [09. Bronze → Silver → Gold](../course/09-pipeline.md).

**준비:** Unity Catalog가 연결된 SQL 실행 환경과 관리형 테이블 생성이 가능한 개인 연습 스키마.

**완료 기준:** Bronze 5행, Silver 5행, Gold 2행이며 재실행해도 금액이 같습니다.

## 1. 개인 연습 공간을 준비합니다

교육 담당자가 제공한 **실제 카탈로그와 개인 스키마 이름**을 사용합니다. [SQL 노트북](../../labs/03_medallion.sql)의 `training_catalog`, `beginner_your_name`은 자리표시자입니다. 예를 들어 제공된 이름이 `edu`와 `beginner_kim`이면 첫 셀을 다음처럼 바꿉니다.

```sql
-- edu와 beginner_kim은 설명용 예시입니다. 본인이 받은 이름을 사용합니다.
USE CATALOG edu;
USE SCHEMA beginner_kim;
SELECT current_catalog(), current_schema();
```

필요한 주요 권한은 `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`입니다. 본인이 만든 테이블의 소유자로 이후 교체·조회를 수행하는 것을 전제로 합니다. 카탈로그/스키마에는 관리형 저장 위치가 준비되어 있어야 합니다.

스키마가 없고 `CREATE SCHEMA` 권한이 있다면 제공된 카탈로그에서 `CREATE SCHEMA beginner_kim;`처럼 개인 이름으로 한 번 만듭니다. 권한이 없으면 담당자가 준비한 스키마를 사용합니다. 카탈로그 생성은 이 실습의 필수 작업이 아닙니다.

**이 실습은 선택한 스키마의 아래 세 테이블을 매번 교체합니다.** 다른 사람과 같은 스키마를 사용하지 않고, 기존 업무 테이블이 아닌지 확인합니다.

- `edu_bronze_orders`
- `edu_silver_orders`
- `edu_gold_category_sales`

## 2. SQL 노트북을 실행합니다

[03_medallion.sql](../../labs/03_medallion.sql)을 Workspace에 Import하고 SQL 실행을 지원하는 컴퓨트에 연결합니다. 또는 SQL Editor의 동일 세션에서 각 구문을 순서대로 실행할 수 있습니다. 전체를 복사할 경우에도 첫 카탈로그·스키마 설정을 먼저 바꿉니다.

| 순서 | 작업 | 기대 결과 |
| --- | --- | --- |
| 1 | 현재 카탈로그·스키마 확인 | 본인의 연습 공간 |
| 2 | Bronze 생성 | 공통 주문 5행 |
| 3 | 품질 검증 | 오류 없음; `assert_true` 결과의 NULL은 정상 |
| 4 | Silver 생성 | 취소를 포함한 유효 주문 5행 |
| 5 | Gold 생성 | `paid` 금액을 집계한 2행 |
| 6–9 | 행 수·금액·정답 확인 | 아래 표와 일치, 검증 오류 없음 |

`assert_true`는 조건이 참이면 NULL을 반환하고 참이 아니면 오류를 발생시킵니다. 검증 셀에서 오류가 나면 다음 셀을 수동으로 실행하지 않습니다. [공식 함수 설명](https://docs.databricks.com/aws/en/sql/language-manual/functions/assert_true)

| 테이블 | 행 수 | 주요 확인 |
| --- | --- | --- |
| Bronze | 5 | 원본과 동일 |
| Silver | 5 | `paid` 3행, `cancelled` 2행 |
| Gold | 2 | book=20000, food=15000, 합계=35000 |

Catalog에서 실제 이름과 열을 확인합니다. 테이블을 저장했으므로 권한이 있는 다른 세션에서도 3단계 이름으로 조회할 수 있습니다.

## 3. 다시 실행합니다

처음부터 동일 입력으로 다시 실행합니다. 결과는 5/5/2행과 35,000원으로 같아야 합니다. 테이블을 교체하므로 누적되지 않습니다. `CREATE OR REPLACE`는 테이블 이력을 남길 수 있어 “결과 데이터가 같다”와 “변경 이력이 없다”는 다른 의미입니다.

입력을 바꾸는 연습은 개인 공간에서만 합니다. 중복 ID나 음수 금액을 넣으면 Bronze 이후 검증에서 중단되어야 합니다. 이전 실행의 Silver·Gold가 남아 있을 수 있으므로 이를 새 입력의 성공 결과로 해석하지 않습니다. 원본 5행으로 복원한 뒤 전체를 재실행합니다.

## 4. 실습을 마칩니다

후속 복습에 필요하면 테이블을 남겨 둡니다. 삭제하기로 했다면 현재 카탈로그·스키마가 개인 연습 공간인지 다시 확인한 뒤 다음 세 이름만 삭제합니다. 관리형 테이블 삭제는 데이터 생명주기에도 영향을 줍니다. 노트북에는 자동 삭제를 넣지 않았습니다.

```sql
SELECT current_catalog(), current_schema();
-- 위 결과가 개인 연습 공간일 때만 아래 구문을 별도로 실행합니다.
DROP TABLE IF EXISTS edu_gold_category_sales;
DROP TABLE IF EXISTS edu_silver_orders;
DROP TABLE IF EXISTS edu_bronze_orders;
```

본인이 시작한 컴퓨트의 종료·자동 종료 설정도 확인합니다.

## 막혔을 때

| 증상 | 확인할 것 |
| --- | --- |
| 카탈로그·스키마가 없음 | 자리표시자를 실제 이름으로 바꿨는지 |
| 권한 오류 | 오류가 가리키는 객체와 `USE`·`CREATE TABLE`·소유권 |
| 관리형 저장 위치 오류 | 담당자에게 해당 스키마의 저장 위치 준비 요청 |
| 검증 결과가 NULL | 오류 없이 반환되었다면 조건 통과 |
| 합계가 다름 | 입력·상태 필터·선택한 스키마·실행 순서 |

[본문으로 돌아가기](../course/09-pipeline.md) · [다음 개념: 10. 분석·AI](../course/10-analytics-and-ai.md) · [실습 목차](README.md)
