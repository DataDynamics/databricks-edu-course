# 05. Python으로 첫 집계를 만듭니다

**학습 목표:** CSV를 읽어 결제 완료 금액을 집계하고 손계산과 비교합니다.

**선수 지식:** [02장](02-data-basics.md)의 집계 기준, [03장](03-storage-and-compute.md)의 로컬 파일, Python 반복문·조건문.

## 실행 전에 결과를 예상합니다

이번 실습은 **로컬 Python 3만 사용**합니다. Databricks·Spark·pandas를 설치하지 않아도 됩니다. [실습 1 안내](../labs/01-local-python.md)를 열어 [실행 파일](../../labs/01_local_sales.py)을 실행합니다.

```bash
python3 labs/01_local_sales.py
```

저장소 최상위 폴더에서 실행하면 다음 결과가 나와야 합니다.

```text
입력 5행 / 결제 완료 3행
book: 20000
food: 15000
전체 결제 완료 금액: 35000
검증 통과
```

## 계산의 중심은 세 단계입니다

아래는 실행 파일의 핵심 부분입니다. `rows`는 CSV를 읽어 만든 행 목록이고, `totals`는 카테고리별 누적 금액입니다.

```python
totals = {}
for row in rows:
    if row["status"] == "paid":
        category = row["category"]
        amount = int(row["amount"])
        totals[category] = totals.get(category, 0) + amount
```

1. **읽기:** `csv.DictReader`로 한 행을 열 이름과 값의 딕셔너리로 읽습니다.
2. **필터:** `if`로 결제 완료 행만 선택합니다.
3. **집계:** 문자열 금액을 정수로 바꾸고 카테고리별 합계에 더합니다.

`totals.get(category, 0)`은 아직 해당 카테고리가 없으면 0에서 시작한다는 뜻입니다. 반복문이 끝나면 결과가 이미 계산되어 있습니다.

## 결과를 확인하는 것까지가 실습입니다

화면에 숫자가 나왔다고 맞는 결과인 것은 아닙니다. 실행 파일은 행 수와 상태를 확인하고, 집계 결과가 `book=20000`, `food=15000`인지 검사합니다. 검사에 실패하면 `AssertionError`가 발생합니다. 이는 고정된 교육 샘플의 정답 확인이며 범용 데이터 품질 시스템은 아닙니다.

이번 파일은 작은 입력을 메모리로 읽습니다. 데이터가 커지면 입력과 중간 결과가 차지하는 메모리, 읽기 속도, 실행 시간이 문제가 됩니다. 파일을 한 행씩 읽어 입력 메모리를 줄일 수도 있지만, 저장소와 전체 계산 규모에 따라 다른 접근이 필요합니다.

## pandas는 언제 등장하나요?

pandas는 Python에서 표를 편리하게 다루는 라이브러리입니다. `filter`에 해당하는 선택과 `groupby` 집계를 간결하게 표현할 수 있습니다. 일반적인 pandas 처리도 한 컴퓨터의 자원을 사용합니다. pandas를 배우는 것이 Spark의 필수 선수 조건은 아니므로 첫 완주 뒤 [세 방식 비교](../python-vs-pyspark.md#basic-examples)에서 확인해도 됩니다.

## 확인 문제

1. `paid` 필터를 없애면 합계가 어떻게 바뀌나요?
2. 이 코드를 Databricks 노트북에 붙이면 반복문이 자동으로 분산되나요?

<details>
<summary>해설</summary>

1. book=27000, food=20000, 총 47000입니다. 취소 주문까지 포함했으므로 이번 질문의 정답은 아닙니다.
2. 아닙니다. 일반 Python 반복문은 해당 Python 프로세스에서 실행됩니다. Spark API로 처리 계획을 표현하는 방법은 다음 장에서 배웁니다.

</details>

[이전: 04. Lakehouse](04-lakehouse.md) · [전체 목차](../../README.md) · [다음: 06. Spark](06-spark.md)
