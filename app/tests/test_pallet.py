"""팔레트 최적화 단위 테스트."""
from __future__ import annotations

from lib.pallet import PalletItem, optimize_to_pallet


def _mk(
    key,
    urgency="stable",
    basic_boxes=1,
    box_qty=10,
    unit_qty=1,
    parent="P1",
    current=0,
    velocity=10.0,             # 기본: box 1개 = 1일분 (cover cap 여유)
    days_until_stockout=20.0,
):
    return PalletItem(
        key=key,
        urgency=urgency,
        basic_boxes=basic_boxes,
        box_qty=box_qty,
        unit_qty=unit_qty,
        parent_barcode=parent,
        current_total_stock=current,
        velocity=velocity,
        days_until_stockout=days_until_stockout,
    )


# 기본 파라미터: 팔레트 20, overstock 상한 크게, legacy auto 라운딩, cap 사실상 무한
# (대부분의 기존 테스트는 auto 모드 + cap 영향 없는 가정)
DEFAULT_KW = dict(pallet_size=20, overstock_days=999, rounding="auto", cap_per_sku=999)


def test_already_aligned_no_change():
    items = [_mk("A", basic_boxes=20)]
    r = optimize_to_pallet(items, {"P1": 10000}, **DEFAULT_KW)
    assert r.mode == "noop"
    assert r.total_boxes_after == 20
    assert r.optimized_boxes["A"] == 20
    assert r.adjustments == []


def test_up_rounding_small_gap():
    """17박스 → 20박스 (올림, 3박스 추가)."""
    items = [
        _mk("A", urgency="replenish", basic_boxes=5),  # 보호 (고정)
        _mk("B", urgency="stable", basic_boxes=8, days_until_stockout=30),
        _mk("C", urgency="stable", basic_boxes=4, days_until_stockout=50),
    ]
    r = optimize_to_pallet(items, {"P1": 10000}, **DEFAULT_KW)
    assert r.mode == "up"
    assert r.total_boxes_after == 20
    assert r.optimized_boxes["A"] == 5
    assert r.optimized_boxes["B"] + r.optimized_boxes["C"] == 15
    assert r.optimized_boxes["B"] > r.optimized_boxes["C"]  # B 가 더 급함(=30) → 더 많이


def test_down_rounding_large_gap():
    """21박스 → 20박스 (내림 우선, 올림폭 19 > 10)."""
    items = [
        _mk("A", urgency="replenish", basic_boxes=5),
        _mk("B", urgency="stable", basic_boxes=10, days_until_stockout=60),
        _mk("C", urgency="stable", basic_boxes=6, days_until_stockout=40),
    ]
    r = optimize_to_pallet(items, {"P1": 10000}, **DEFAULT_KW)
    assert r.mode == "down"
    assert r.total_boxes_after == 20
    assert r.optimized_boxes["A"] == 5
    # 여유있는 B(60일) 가 먼저 -1
    assert r.optimized_boxes["B"] == 9
    assert r.optimized_boxes["C"] == 6


def test_protected_never_modified():
    items = [
        _mk("X", urgency="critical", basic_boxes=17),
        _mk("Y", urgency="stable", basic_boxes=2, days_until_stockout=40),
    ]
    r = optimize_to_pallet(items, {"P1": 10000}, **DEFAULT_KW)
    assert r.optimized_boxes["X"] == 17
    assert r.optimized_boxes["Y"] == 3
    assert r.total_boxes_after == 20


def test_cover_days_cap_blocks_addition():
    """cover_days 상한으로 더 이상 추가 불가 (up 모드 강제)."""
    # velocity=1개/일, basic=15박스=150개 → 현재0 + 150 = cover 150일... 너무 많다
    # 다시: basic=15박스, box=1, velocity=1 → 15개 cover 15일, +5박스 → 20일 OK, 추가 시 40일 OK? 35 cap → +1 (16) OK ≤35일, +5 (20) 20일 OK.
    # 실제로 cap 을 타이트하게 걸려면: basic=15, velocity=1, box=1 → +15 까지 OK (30일), +20 이 35일 경계
    # basic=15, pallet=20, up_delta=5, overstock=20 (=tight) → +5 가능? 15+5=20 → cover 20 OK
    # +6 부터 21일 초과. 5박스는 커버 가능
    items = [
        _mk("A", urgency="stable", basic_boxes=15, box_qty=1, current=0, velocity=1.0, days_until_stockout=15),
    ]
    r = optimize_to_pallet(
        items, {"P1": 10000}, pallet_size=20, overstock_days=20, rounding="up", cap_per_sku=999
    )
    # 목표 20, delta 5, cover 상한 20 → 5박스 정확히 추가 가능 (총 20일=상한)
    assert r.optimized_boxes["A"] == 20
    assert r.unfilled == 0

    # 더 타이트한 케이스: 상한 18 → 3박스만 가능
    r2 = optimize_to_pallet(
        items, {"P1": 10000}, pallet_size=20, overstock_days=18, rounding="up", cap_per_sku=999
    )
    assert r2.optimized_boxes["A"] == 18
    assert r2.unfilled == 2


def test_parent_pool_constraint():
    """부모 풀 낱개 부족 → 추가 불가 (up 강제)."""
    items = [
        _mk("A", urgency="stable", basic_boxes=18, box_qty=10, unit_qty=1, parent="P1"),
    ]
    # basic=18, up_delta=2, 풀 낱개=5 < 10 → 추가 불가
    r = optimize_to_pallet(
        items, {"P1": 5}, pallet_size=20, overstock_days=999, rounding="up", cap_per_sku=999
    )
    assert r.optimized_boxes["A"] == 18
    assert r.unfilled == 2


def test_bundle_consumes_more_pool():
    """2개입 번들: 1박스당 base 낱개 2배 소모 (up 강제)."""
    items = [
        _mk("A", urgency="stable", basic_boxes=15, box_qty=10, unit_qty=2, parent="P1"),
    ]
    # basic=15, up_delta=5, 각 박스 20 낱개 소모. 풀 30 → 1박스만 가능
    r = optimize_to_pallet(
        items, {"P1": 30}, pallet_size=20, overstock_days=999, rounding="up", cap_per_sku=999
    )
    assert r.optimized_boxes["A"] == 16
    assert r.unfilled == 4


def test_no_candidates_up_mode():
    items = [_mk("X", urgency="critical", basic_boxes=15)]
    r = optimize_to_pallet(items, {"P1": 10000}, **DEFAULT_KW)
    assert r.mode == "up"
    assert r.unfilled == 5
    assert r.optimized_boxes["X"] == 15


def test_total_zero_noop():
    items = [_mk("A", basic_boxes=0)]
    r = optimize_to_pallet(items, {"P1": 100}, **DEFAULT_KW)
    assert r.mode == "noop"
    assert r.total_boxes_after == 0


def test_round_robin_distribution():
    """두 후보가 비슷한 우선순위면 박스가 균등 분산."""
    items = [
        _mk("B", urgency="stable", basic_boxes=5, days_until_stockout=30.0),
        _mk("C", urgency="stable", basic_boxes=5, days_until_stockout=30.1),
    ]
    # total=10, up_delta=10 (올림폭 10/20=0.5, 임계치 0.5 "초과" 조건 → up 모드 유지)
    r = optimize_to_pallet(items, {"P1": 10000}, **DEFAULT_KW)
    assert r.mode == "up"
    assert r.total_boxes_after == 20
    diff = abs(r.optimized_boxes["B"] - r.optimized_boxes["C"])
    assert diff <= 1, f"분산 실패: B={r.optimized_boxes['B']}, C={r.optimized_boxes['C']}"


def test_auto_rounding_threshold_exactly_half():
    """(legacy auto) 올림폭이 팔레트의 정확히 절반이면 up 모드."""
    items = [_mk("A", urgency="stable", basic_boxes=10)]
    r = optimize_to_pallet(
        items, {"P1": 10000}, pallet_size=20, overstock_days=999, rounding="auto", cap_per_sku=999
    )
    assert r.mode == "up"


def test_auto_rounding_prefers_down_when_gap_large():
    """(legacy auto) 올림폭이 팔레트 절반을 초과하면 down 모드."""
    # total = 9, up_delta = 11, 11/20 = 0.55 > 0.5 → down
    items = [_mk("A", urgency="stable", basic_boxes=9)]
    r = optimize_to_pallet(
        items, {"P1": 10000}, pallet_size=20, overstock_days=999, rounding="auto", cap_per_sku=999
    )
    assert r.mode == "down"
    assert r.total_boxes_after == 0 or r.total_boxes_after == 9 - 9  # 9 removed → 0


def test_default_always_up_fills_pallet():
    """신규 기본 동작: rounding='up' 이 기본이며 항상 올림."""
    # total=9, up_delta=11. 과거 auto 라면 down 이지만 이제 up 이어야 함.
    items = [
        _mk("A", urgency="stable", basic_boxes=5, days_until_stockout=30),
        _mk("B", urgency="stable", basic_boxes=4, days_until_stockout=40),
    ]
    r = optimize_to_pallet(
        items, {"P1": 10000}, pallet_size=20, overstock_days=999, cap_per_sku=999
    )
    assert r.mode == "up"
    assert r.total_boxes_after == 20


def test_cap_per_sku_prevents_concentration():
    """cap_per_sku=2 로 한 SKU 에 2박스 초과 추가 금지."""
    # 후보 2개인데 +4 필요. cap=2 면 각 +2 까지만.
    items = [
        _mk("A", urgency="stable", basic_boxes=8, days_until_stockout=10),  # 가장 급함
        _mk("B", urgency="stable", basic_boxes=7, days_until_stockout=50),
    ]
    r = optimize_to_pallet(
        items, {"P1": 10000}, pallet_size=19, overstock_days=999, cap_per_sku=2
    )
    # basic 15 → 목표 19 (+4). cap=2 → A +2, B +2
    assert r.optimized_boxes["A"] == 10
    assert r.optimized_boxes["B"] == 9
    assert r.total_boxes_after == 19


def test_cap_per_sku_unfilled_when_too_few_candidates():
    """cap_per_sku=2 인데 후보 1개뿐이면 +2 넘어서 못 채움."""
    items = [
        _mk("A", urgency="stable", basic_boxes=15, days_until_stockout=10),
    ]
    r = optimize_to_pallet(
        items, {"P1": 10000}, pallet_size=19, overstock_days=999, cap_per_sku=2
    )
    # +4 필요하지만 cap 2 로 +2 만 → unfilled 2
    assert r.optimized_boxes["A"] == 17
    assert r.unfilled == 2
