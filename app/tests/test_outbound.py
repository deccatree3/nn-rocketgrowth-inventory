"""출고 배치 선택 로직 단위 테스트."""
from __future__ import annotations

from datetime import date

from lib.outbound import select_outbound_batch


SHORT = date(2028, 2, 4)
LONG = date(2028, 3, 18)


def test_no_demand():
    r = select_outbound_batch(0, [{"expiry": SHORT, "available": 100}])
    assert r.status == "no_demand"


def test_no_batch():
    r = select_outbound_batch(50, [])
    assert r.status == "no_batch"
    assert r.available == 0


def test_short_covers_request():
    """짧은 배치가 요청을 단독으로 커버 가능 → 짧은 것 선택 (FIFO)."""
    batches = [
        {"expiry": SHORT, "available": 100},
        {"expiry": LONG, "available": 500},
    ]
    r = select_outbound_batch(50, batches)
    assert r.status == "ok"
    assert r.expiry == SHORT
    assert r.available == 100


def test_short_exactly_covers():
    """짧은 배치 수량 == 요청 수량 (경계)."""
    batches = [
        {"expiry": SHORT, "available": 50},
        {"expiry": LONG, "available": 500},
    ]
    r = select_outbound_batch(50, batches)
    assert r.status == "ok"
    assert r.expiry == SHORT


def test_short_insufficient_long_covers():
    """사용자 예시: 짧은30 긴100 → 50 출고 → 긴배치 사용."""
    batches = [
        {"expiry": SHORT, "available": 30},
        {"expiry": LONG, "available": 100},
    ]
    r = select_outbound_batch(50, batches)
    assert r.status == "ok"
    assert r.expiry == LONG
    assert r.available == 100


def test_neither_covers_alone():
    """두 배치 모두 요청을 단독으로 커버 불가 → insufficient."""
    batches = [
        {"expiry": SHORT, "available": 30},
        {"expiry": LONG, "available": 40},
    ]
    r = select_outbound_batch(100, batches)
    assert r.status == "insufficient"
    assert r.largest_available == 40
    assert r.note is not None


def test_single_batch_only():
    batches = [{"expiry": SHORT, "available": 200}]
    r = select_outbound_batch(100, batches)
    assert r.status == "ok"
    assert r.expiry == SHORT
    assert r.available == 200


def test_multiple_batches_choose_earliest_feasible():
    """여러 배치가 모두 커버 가능하면 가장 빠른 것 선택."""
    batches = [
        {"expiry": date(2028, 1, 1), "available": 100},
        {"expiry": date(2028, 6, 1), "available": 200},
        {"expiry": date(2028, 12, 1), "available": 300},
    ]
    r = select_outbound_batch(50, batches)
    assert r.expiry == date(2028, 1, 1)


def test_user_example_parent_pool_allocation():
    """사용자 예시: A 재고 100, A 1번들 20 확정 → A 2번들 가능 40개(=80/2).

    단일 배치만 있는 단순 시나리오.
    """
    from lib.outbound import (
        PoolAllocationItem,
        allocate_parent_pool,
    )

    batches = [{"expiry": date(2028, 1, 1), "available": 100}]
    items = [
        PoolAllocationItem(key="A_1pack", unit_qty=1, requested_qty=20),
        PoolAllocationItem(key="A_2pack", unit_qty=2, requested_qty=0),  # 아직 확정 안함
    ]
    results, _updated = allocate_parent_pool(items, batches)
    # 1번들 확정 후 풀 잔여: 100 - 20 = 80
    r1 = next(r for r in results if r.key == "A_1pack")
    assert r1.status == "ok"
    assert r1.pool_remaining_base_after == 80

    # 2번들 시점 풀 잔여도 80 (확정 수량 0)
    r2 = next(r for r in results if r.key == "A_2pack")
    assert r2.status == "no_demand"
    assert r2.pool_remaining_base_after == 80
    # 2번들 가능 수량 = 80 / 2 = 40 (max_single_batch_after / unit_qty)
    assert r2.max_single_batch_after // r2.unit_qty == 40


def test_parent_pool_with_multi_batches_fifo():
    """부모 풀에 2 배치 존재 시 FIFO로 할당."""
    from lib.outbound import PoolAllocationItem, allocate_parent_pool

    batches = [
        {"expiry": date(2028, 2, 4), "available": 100},
        {"expiry": date(2028, 3, 18), "available": 500},
    ]
    items = [
        PoolAllocationItem(key="1pack", unit_qty=1, requested_qty=60),  # 짧은 배치 사용
        PoolAllocationItem(key="2pack", unit_qty=2, requested_qty=30),  # 60 낱개, 짧은배치 잔여 40 < 60 → 긴배치
    ]
    results, _ = allocate_parent_pool(items, batches)
    assert results[0].selected_batch_expiry == date(2028, 2, 4)
    assert results[0].status == "ok"
    assert results[1].selected_batch_expiry == date(2028, 3, 18)
    assert results[1].status == "ok"


def test_parent_pool_insufficient_marks_item():
    """단일 배치로 커버 불가한 아이템은 insufficient."""
    from lib.outbound import PoolAllocationItem, allocate_parent_pool

    batches = [
        {"expiry": date(2028, 2, 4), "available": 30},
        {"expiry": date(2028, 3, 18), "available": 40},
    ]
    items = [
        PoolAllocationItem(key="big", unit_qty=1, requested_qty=100),
    ]
    results, _ = allocate_parent_pool(items, batches)
    assert results[0].status == "insufficient"
    assert results[0].max_single_batch_after == 40


def test_expiry_none_batch_last():
    """유통일 None 배치는 마지막 순번."""
    batches = [
        {"expiry": None, "available": 500},  # 유통일 미상
        {"expiry": SHORT, "available": 30},
    ]
    # 30 요청 → SHORT가 먼저 (30 >= 30)
    r = select_outbound_batch(30, batches)
    assert r.expiry == SHORT
    # 50 요청 → SHORT 부족, None 배치 500 선택
    r2 = select_outbound_batch(50, batches)
    assert r2.expiry is None
    assert r2.available == 500
