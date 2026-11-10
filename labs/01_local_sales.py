"""실습 1: Python 표준 라이브러리로 공통 주문 CSV를 집계합니다."""

import csv
from pathlib import Path


# 실행한 터미널의 위치 대신 이 파일의 위치를 기준으로 CSV를 찾습니다.
data_path = Path(__file__).resolve().parents[1] / "data" / "orders.csv"
with data_path.open(encoding="utf-8", newline="") as source:
    rows = list(csv.DictReader(source))

# 아래 assert는 고정된 교육 샘플을 확인하는 검사입니다.
assert len(rows) == 5, "공통 샘플은 5행이어야 합니다."
order_ids = [int(row["order_id"]) for row in rows]
assert len(set(order_ids)) == len(rows), "중복된 주문 ID가 있습니다."
assert all(order_id > 0 for order_id in order_ids), "주문 ID는 양수입니다."
for row in rows:
    assert row["category"].strip(), "카테고리가 비어 있습니다."
    assert row["status"] in {"paid", "cancelled"}, "알 수 없는 주문 상태입니다."
    assert int(row["amount"]) >= 0, "금액은 0 이상이어야 합니다."

totals = {}
for row in rows:
    if row["status"] == "paid":
        category = row["category"]
        amount = int(row["amount"])
        totals[category] = totals.get(category, 0) + amount

paid_count = sum(row["status"] == "paid" for row in rows)
assert paid_count == 3, "결제 완료 주문 수가 예상과 다릅니다."
assert totals == {"book": 20000, "food": 15000}, "집계 결과가 예상과 다릅니다."

print(f"입력 {len(rows)}행 / 결제 완료 {paid_count}행")
for category, amount in sorted(totals.items()):
    print(f"{category}: {amount}")
print(f"전체 결제 완료 금액: {sum(totals.values())}")
print("검증 통과")
