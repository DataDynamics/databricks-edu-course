# 08. Unity Catalog로 이름과 권한을 관리합니다

**학습 목표:** 테이블의 3단계 이름과 조회·생성에 필요한 권한을 이해합니다.

**선수 지식:** [07장](07-databricks.md)의 세션·임시 뷰·컴퓨트.

## 임시 결과를 팀의 데이터로 만들려면

세션이 끝나도 결과를 남기고 다른 사람이 조회하려면 저장 위치뿐 아니라 이름·설명·소유자·접근 권한을 관리해야 합니다. Databricks의 Unity Catalog는 이 데이터 자산 관리를 담당합니다.

```text
카탈로그.스키마.테이블
training_catalog.beginner_your_name.edu_gold_category_sales
```

위 이름은 **구조 예시**입니다. `training_catalog`와 `beginner_your_name`은 실제 실습용 이름으로 바꿉니다.

| 부분 | 역할 |
| --- | --- |
| Catalog | 큰 관리·권한 경계 |
| Schema | 카탈로그 안에서 테이블 등을 묶는 공간 |
| Table | 행·열 형태로 조회할 데이터 자산 |

02장의 “스키마 = 열과 자료형”과 여기의 “스키마 = 이름 공간”은 같은 단어의 다른 용법입니다. 문맥을 보고 구분합니다.

## 파일·테이블·볼륨을 구분합니다

| 대상 | 쓰임 |
| --- | --- |
| Table | SQL·DataFrame으로 행·열 조회 |
| Volume | CSV·PDF·이미지처럼 파일 단위 자산 관리 |
| Managed table | 플랫폼이 데이터 파일의 저장 위치와 생명주기를 관리 |
| External table | 지정한 외부 저장 위치의 데이터를 테이블로 등록·관리 |

이번 실습은 경로를 직접 지정하지 않는 **관리형 Delta 테이블**을 만듭니다. 연습 공간에는 관리형 테이블을 저장할 위치가 준비되어 있어야 합니다. 세부 저장소 설정은 관리자가 준비합니다. [관리형 테이블 안내](https://docs.databricks.com/aws/en/tables/managed)

## 이름이 보이는 것과 읽을 수 있는 것은 다릅니다

| 하려는 일 | 필요한 주요 권한 |
| --- | --- |
| 테이블 조회 | 상위의 `USE CATALOG`, `USE SCHEMA`와 대상의 `SELECT` |
| 기존 스키마에 테이블 생성 | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` |
| 카탈로그 아래 연습 스키마 생성 | `USE CATALOG`, `CREATE SCHEMA` |

권한은 사용자·그룹·서비스 주체에 부여할 수 있습니다. 상위에서 상속된 권한도 영향을 줍니다. 생성한 테이블을 수정·교체할 때는 소유권 등 해당 작업 권한이 필요하므로 실습은 본인의 전용 공간에서 진행합니다. [Unity Catalog 권한](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)

처음부터 카탈로그를 직접 생성하거나 권한을 부여할 필요는 없습니다. 관리자가 기존 연습 카탈로그와 개인 스키마를 제공하면 그것을 사용합니다. “권한 없음” 오류를 만났다면 어떤 객체에 어떤 동작이 거절되었는지 확인합니다.

## 데이터가 어디서 왔는지도 설명합니다

**계보(lineage)**는 원본과 결과의 연결 관계입니다. **감사(audit)**는 누가 어떤 작업을 수행했는지에 관한 기록입니다. 계보가 보인다고 데이터 값이 정확하다는 보장은 없으므로 품질 검사는 별도로 해야 합니다. 행 필터·열 마스크·ABAC·외부 공유는 첫 테이블 실습 뒤에 확장합니다.

## 확인 문제

1. `USE CATALOG`만 있으면 모든 테이블 내용을 읽을 수 있나요?
2. 실습 2의 임시 뷰와 관리형 테이블은 어떻게 다른가요?

<details>
<summary>해설</summary>

1. 아닙니다. 상위 스키마 사용 권한과 대상 테이블 조회 권한도 필요합니다.
2. 임시 뷰는 세션 안의 조회 이름입니다. 관리형 테이블은 데이터를 지속적으로 보관하며 카탈로그 이름·권한으로 접근합니다.

</details>

**더 알아보기:** [객체 구조](../unity-catalog.md#object-model), [권한 상속](../unity-catalog.md#privileges), [Managed·External과 볼륨](../unity-catalog.md#managed-external).

[이전: 07. Databricks](07-databricks.md) · [전체 목차](../../README.md) · [다음: 09. 파이프라인](09-pipeline.md)
