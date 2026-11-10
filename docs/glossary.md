# 입문 핵심 용어집

처음 완주할 때 필요한 용어만 모았습니다. 전체 기술 용어는 [심화 자료](reference/README.md)의 각 용어집에서 확인합니다.

## 데이터와 저장

| 용어 | 짧은 뜻 | 처음 배우는 곳 |
| --- | --- | --- |
| 파이프라인 | 원본을 필요한 결과로 만드는 연결된 처리 단계 | [01장](course/01-data-engineering.md) |
| 스냅샷 | 한 시점의 상태를 담은 데이터 | [01장](course/01-data-engineering.md) |
| 행·열 | 한 건의 기록과 그 속성 | [02장](course/02-data-basics.md) |
| Grain | 한 행이 나타내는 단위 | [02장](course/02-data-basics.md) |
| 키 | 행을 식별하는 값 | [02장](course/02-data-basics.md) |
| 스키마: 데이터 구조 | 열 이름·자료형 등 구조의 약속 | [02장](course/02-data-basics.md) |
| NULL | 값이 없거나 알려지지 않았음을 표현; 0과 다름 | [02장](course/02-data-basics.md) |
| 객체 저장소 | 버킷과 키 등으로 객체를 저장·조회하는 저장소 | [03장](course/03-storage-and-compute.md) |
| Parquet | 열 단위 데이터 표현을 사용하는 파일 형식 | [03장](course/03-storage-and-compute.md) |
| OLTP / OLAP | 업무 처리 / 분석 처리 | [04장](course/04-lakehouse.md) |
| DW / Data Mart | 통합 분석 데이터 / 특정 주제·팀의 분석 범위 | [04장](course/04-lakehouse.md) |
| Data Lake / Lakehouse | 다양한 데이터 보관 기반 / 여기에 테이블 관리·분석을 결합한 구조 | [04장](course/04-lakehouse.md) |
| Delta Lake | 데이터 파일과 로그로 일관된 테이블 변경·버전을 관리하는 기술 | [04장](course/04-lakehouse.md) |

## 처리와 플랫폼

| 용어 | 짧은 뜻 | 처음 배우는 곳 |
| --- | --- | --- |
| pandas | Python에서 표 형태의 데이터를 처리하는 라이브러리 | [05장](course/05-python.md) |
| Spark / PySpark | 분산 처리 엔진 / 이를 사용하는 Python API | [06장](course/06-spark.md) |
| DataFrame | 행·열 형태의 데이터 표현; Spark에서는 처리 계획과 연결 | [06장](course/06-spark.md) |
| Driver / Executor | 실행을 조율하는 역할 / 배정된 작업을 수행하는 역할 | [06장](course/06-spark.md) |
| Partition / Task | 나눈 데이터 단위 / 실행 작업 단위 | [06장](course/06-spark.md) |
| Shuffle | 집계·조인 등을 위해 파티션 간 데이터를 재분배 | [06장](course/06-spark.md) |
| Transformation / Action | 처리 계획 구성 / 결과 조회·저장 등 실행 요청 | [06장](course/06-spark.md) |
| 지연 실행 / OOM | 결과가 필요할 때 계산 / 필요한 메모리 부족 | [06장](course/06-spark.md) |
| Workspace / Notebook | 작업 공간 / 코드·설명·결과를 셀로 묶은 문서 | [07장](course/07-databricks.md) |
| Compute / SQL Warehouse | 실행 자원 / SQL 실행용 컴퓨트 | [07장](course/07-databricks.md) |
| 임시 뷰 | 현재 세션에서 SQL로 조회할 이름 | [07장](course/07-databricks.md) |
| Unity Catalog | 데이터 자산의 이름·권한·설명·계보 등을 관리 | [08장](course/08-unity-catalog.md) |
| Catalog / Schema: 이름 공간 | 큰 관리 범위 / 그 안의 객체 묶음 | [08장](course/08-unity-catalog.md) |
| Managed / External | 플랫폼이 파일 위치·생명주기를 관리 / 외부 위치를 지정해 관리 | [08장](course/08-unity-catalog.md) |
| Volume | 테이블이 아닌 파일을 관리하는 객체 | [08장](course/08-unity-catalog.md) |
| Lineage / Audit | 원본·결과의 연결 / 누가 어떤 작업을 했는지의 기록 | [08장](course/08-unity-catalog.md) |

## 품질·활용·운영

| 용어 | 짧은 뜻 | 처음 배우는 곳 |
| --- | --- | --- |
| Bronze / Silver / Gold | 원본 / 검증된 상세 / 목적별 제공 단계 | [09장](course/09-pipeline.md) |
| 멱등성 | 같은 입력을 반복 반영해도 결과 데이터가 같도록 하는 성질 | [09장](course/09-pipeline.md) |
| 증분 처리 | 새로 생기거나 바뀐 범위만 반영하는 처리 | [09장](course/09-pipeline.md) |
| BI | 보고서·대시보드 등을 통한 업무 분석 | [10장](course/10-analytics-and-ai.md) |
| Feature / Label | 모델 입력 / 맞히려는 정답 | [10장](course/10-analytics-and-ai.md) |
| 학습 / 추론 | 데이터로 모델 조정 / 모델로 새 입력의 결과 계산 | [10장](course/10-analytics-and-ai.md) |
| RAG | 관련 근거를 검색해 생성형 모델의 입력에 함께 제공 | [10장](course/10-analytics-and-ai.md) |
| 청킹 / 임베딩 | 문서를 검색 단위로 나눔 / 의미를 수치 벡터로 표현 | [10장](course/10-analytics-and-ai.md) |
| 배치 / 스트리밍 | 일정 범위를 묶어 처리 / 계속 들어오는 데이터를 지속 처리 | [11장](course/11-next-steps.md) |

[전체 목차](../README.md)
