# 후속 교육과정과 현재 범위

입문 과정은 전체 지도를 이해하고 작은 배치 처리를 실행하는 데 초점을 둡니다. 아래는 **앞으로 확장할 순서**이며, 모두 구현되어 있다는 뜻은 아닙니다. 일정은 아직 정하지 않았습니다.

## 현재 자료로 더 공부할 수 있는 주제

| 주제 | 먼저 이해할 것 | 현재 자료와 범위 |
| --- | --- | --- |
| SQL·데이터 모델링 | 02·04장 | [팩트·차원·DW·마트 설명](data-engineering-lakehouse-ai.md#db-mart-warehouse); JOIN·윈도 함수 실습은 미제공 |
| pandas와 Spark 비교 | 05·06장 | [세 방식 집계 예제](python-vs-pyspark.md#basic-examples) |
| 저장소별 읽기 | 03·05장 | [NAS·NFS·HDFS·S3 예제](storage-systems-and-python.md); 준비된 저장소·인증 필요 |
| Spark 런타임 | 06장 | [JVM·Python worker·Spark Connect 설명](python-pyspark-runtime.md) |
| JDBC 병렬 읽기 | 06·07장 | [MySQL 실습](python-vs-pyspark.md#mysql-examples); DB·드라이버·네트워크·Secret 준비 필요 |
| 분산 ML 원리 | 06·10장 | [K-means 개념 비교](python-vs-pyspark.md#distributed-clustering); 실행형 모델 학습 실습은 미제공 |
| 세밀한 거버넌스 | 08장 | [행 필터·열 마스크·ABAC](unity-catalog.md#fine-grained), [Federation·공유](unity-catalog.md#federation-sharing); 별도 권한·환경 필요 |
| 플랫폼 기능 확장 | 07·09·10장 | [Lakeflow·BI·AI·비용 개요](databricks-platform.md); 전체 운영 구축 실습은 미제공 |

## 앞으로 작성할 실습

| 권장 순서 | 후속 모듈 | 앞에서 필요한 것 | 만들 결과 | 현재 상태 |
| --- | --- | --- | --- | --- |
| 1 | SQL·Python 기초 보강 | 02·05장 | JOIN·NULL·날짜·함수·예외를 다루는 연습 | 별도 강좌 미작성 |
| 2 | 실제 파일 수집과 Delta 변경 | 03·08·09장 | 볼륨 입력 → 테이블, 변경·삭제·이력 확인 | 부분 예제만 있음 |
| 3 | 증분 처리와 CDC | 2번 모듈 | 주문 상태 변경을 중복 없이 반영 | 완결 실습 미작성 |
| 4 | 작업 자동화와 복구 | 3번 모듈 | 예약·재시도·품질 실패·알림·재처리 | 완결 실습 미작성 |
| 5 | 분석 모델과 대시보드 | 1·2번 모듈 | 날짜·환불 정의를 갖춘 일별 지표와 차트 | 완결 실습 미작성 |
| 6 | 분산 처리 성능 | 06장과 실제 규모의 입력 | 조인·Shuffle·편중·실행 계획 비교 | 개념 자료만 있음 |
| 7 | 스트리밍 | 3·4번 모듈 | 지속 입력·체크포인트·늦은 데이터 처리 | 완결 실습 미작성 |
| 8 | ML 또는 RAG | 4번 모듈과 10장 | 학습/평가/서빙 또는 문서 검색/응답/평가 | 흐름 설명만 있음 |

CDC는 DB에서 생긴 변경을 포착해 전달하는 방식입니다. ML과 RAG는 목적에 따라 선택하는 갈래이며 둘을 반드시 연속해서 배울 필요는 없습니다. 스트리밍도 모든 데이터 파이프라인의 필수 조건은 아닙니다.

후속 모듈을 추가할 때는 선수 지식, 환경·권한, 입력, 기대 결과, 재실행·정리 방법을 함께 작성합니다. 실제 실습 파일과 실행 안내가 준비되기 전에는 “실습 제공”으로 표시하지 않습니다.

[이전: 11. 복습](course/11-next-steps.md) · [전체 목차](../README.md) · [심화 자료 색인](reference/README.md)
