# 심화 자료 찾아보기

기존 6개 문서는 상세 설명·PNG·확장 예제를 담은 참고 자료입니다. **첫 학습 순서는 [입문 과정](../../README.md)**을 따릅니다. 아래 문서를 처음부터 모두 읽어야 다음 입문 장으로 갈 수 있는 것은 아닙니다.

## 입문 과정과 기존 문서의 연결

| 기존 문서 | 연결되는 입문 장 | 필요할 때 읽을 부분 |
| --- | --- | --- |
| [Data Engineering에서 Lakehouse와 AI까지](../data-engineering-lakehouse-ai.md) | 01·04·09·10 | 역할, DW·마트 모델, Lakehouse, 품질, ML·RAG |
| [저장소와 Python 파일 집계](../storage-systems-and-python.md) | 03·05 | NAS·NFS 구분, HDFS·S3 연결, 저장과 처리 |
| [Python과 PySpark 차이](../python-vs-pyspark.md) | 05·06 | pandas 비교, 병렬 처리, Action·OOM, JDBC, K-means |
| [Python·PySpark 런타임](../python-pyspark-runtime.md) | 06 이후 선택 | JVM, Py4J, Python worker, Spark Connect |
| [Databricks 플랫폼](../databricks-platform.md) | 07·10·11 | 아키텍처, 컴퓨트, Lakeflow, BI·AI, 비용 |
| [Unity Catalog](../unity-catalog.md) | 08·09 | 객체, Managed·External, 권한 상속, 행·열 제어, 계보, 외부 공유 |

## 궁금한 질문에서 바로 찾기

| 질문 | 상세 설명 |
| --- | --- |
| 분산 저장소에서 읽는데 왜 Python이 느릴까요? | [분산 저장과 분산 처리](../storage-systems-and-python.md#storage-vs-compute) |
| 작은 데이터에서도 Spark가 더 빠른가요? | [병렬 처리의 비용](../python-vs-pyspark.md#parallel-processing-benefits) |
| `collect()`와 `count()`는 무엇이 다른가요? | [지연 실행과 Action](../python-vs-pyspark.md#transformations-and-actions) |
| Python으로 작성하면 실제 계산도 Python이 하나요? | [실행 위치](../python-pyspark-runtime.md#execution-location) |
| MySQL은 Driver가 모두 읽어서 나눠 주나요? | [JDBC 읽기와 집계](../python-vs-pyspark.md#mysql-examples) |
| Data Mart는 별도 서버여야 하나요? | [DB·DW·마트 비교](../data-engineering-lakehouse-ai.md#db-mart-warehouse) |
| 테이블이 보여도 왜 조회가 거절되나요? | [권한과 상속](../unity-catalog.md#privileges) |
| 외부 DB를 연결하는 것과 공유하는 것은 무엇이 다른가요? | [Federation과 공유](../unity-catalog.md#federation-sharing) |
| 어떤 컴퓨트가 필요한가요? | [컴퓨트 유형](../databricks-platform.md#compute) |

## 심화 예제의 준비 조건

기존 문서의 `shop_prod`, S3 경로, DB 주소, 그룹 이름 등은 설명용입니다. 본인의 환경과 준비된 객체·권한에 맞게 바꿉니다. 입문 실습의 개인 스키마와 같은 공간이라고 가정하지 않습니다.

특히 기존 Unity Catalog 문서의 종합 실습은 여러 관리 기능을 연습하는 확장 과제이며, 코드 조각만으로 모든 테이블·사용자·정책을 생성하는 완결 노트북은 아닙니다. 첫 실행은 [입문 실습 3](../labs/03-medallion.md)을 사용합니다. 플랫폼 문서의 수집·AI 예제도 각 원본·권한·서비스 준비가 필요합니다.

[전체 목차](../../README.md) · [후속 교육과정](../roadmap.md)
