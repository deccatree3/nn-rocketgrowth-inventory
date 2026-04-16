"""팔레트 배분 알고리즘 테스트.

규칙:
- 박스수 ≥ 20 SKU 는 단독 팔레트로 분할 (잔여는 잔여풀로)
- 잔여풀은 박스수 내림차순으로 First-Fit Skip-Ahead
- 같은 SKU < 20 박스는 분할 금지
"""
from __future__ import annotations

from lib.pallet_assign import PalletItem, assign_pallets


def _i(key, name, boxes):
    return PalletItem(key=key, name=name, boxes=boxes)


def _flatten(assignment):
    """디버그용: [(pallet_no, key, boxes), ...]"""
    out = []
    for i, p in enumerate(assignment.pallets, start=1):
        for e in p:
            out.append((i, e.key, e.boxes))
    return out


def test_empty():
    r = assign_pallets([], pallet_size=20)
    assert r.pallet_count == 0
    assert r.total_boxes == 0


def test_single_small_sku():
    r = assign_pallets([_i("A", "A", 5)], pallet_size=20)
    assert r.pallet_count == 1
    assert r.pallets[0][0].boxes == 5


def test_single_full_pallet():
    """박스수 = 20: 단독 팔레트 1개, 잔여 0."""
    r = assign_pallets([_i("A", "A", 20)], pallet_size=20)
    assert r.pallet_count == 1
    assert r.pallets[0][0].key == "A"
    assert r.pallets[0][0].boxes == 20


def test_split_unavoidable_24_to_20_plus_4():
    """24박스 → 단독 팔레트 1(20) + 잔여 4(다음 팔레트)."""
    r = assign_pallets([_i("A", "A", 24)], pallet_size=20)
    assert r.pallet_count == 2
    # 첫 팔레트: A 20
    assert r.pallets[0] == r.pallets[0]
    assert any(e.key == "A" and e.boxes == 20 for e in r.pallets[0])
    # 둘째 팔레트: A 4
    assert any(e.key == "A" and e.boxes == 4 for e in r.pallets[1])
    assert r.total_boxes == 24


def test_60_to_three_full_pallets():
    """60박스 = 단독 팔레트 3개, 잔여 0."""
    r = assign_pallets([_i("A", "A", 60)], pallet_size=20)
    assert r.pallet_count == 3
    for p in r.pallets:
        assert sum(e.boxes for e in p) == 20
        assert all(e.key == "A" for e in p)


def test_no_split_for_small_sku():
    """5박스가 2+3 으로 쪼개지지 않음 (분할 금지)."""
    items = [_i("A", "A", 5), _i("B", "B", 18)]
    r = assign_pallets(items, pallet_size=20)
    # 18 + 5 = 23 > 20 → A 는 새 팔레트로 통째로 가야 함
    assert r.pallet_count == 2
    # B 단독 또는 A+다른것 같은 분배
    pallets_with_a = [p for p in r.pallets if any(e.key == "A" for e in p)]
    assert len(pallets_with_a) == 1
    a_entries = [e for p in r.pallets for e in p if e.key == "A"]
    assert len(a_entries) == 1
    assert a_entries[0].boxes == 5


def test_skip_ahead_finds_smaller_item():
    """10,8,7,5,3,2 → 팔레트1=20(10+8+2), 팔레트2=15(7+5+3)"""
    items = [
        _i("a", "a", 10),
        _i("b", "b", 8),
        _i("c", "c", 7),
        _i("d", "d", 5),
        _i("e", "e", 3),
        _i("f", "f", 2),
    ]
    r = assign_pallets(items, pallet_size=20)
    assert r.pallet_count == 2
    # 첫 팔레트: 10,8,2 (skip 7,5,3 because they don't fit)
    p1_keys = sorted(e.key for e in r.pallets[0])
    assert p1_keys == ["a", "b", "f"]
    assert sum(e.boxes for e in r.pallets[0]) == 20
    # 둘째 팔레트: 7,5,3
    p2_keys = sorted(e.key for e in r.pallets[1])
    assert p2_keys == ["c", "d", "e"]
    assert sum(e.boxes for e in r.pallets[1]) == 15


def test_sample_data_replication():
    """샘플 (4. 2차결과물) 의 14 SKU 67박스 시나리오 재현."""
    items = [
        _i(1, "데일리키토 방탄커피(번들/4개입)", 26),  # 20 + 6 잔여
        _i(2, "데일리키토 방탄커피(14포)", 21),       # 20 + 1 잔여
        _i(3, "스키니퓨리티 슈링티(30T)(번들/2개입)", 6),
        _i(4, "스키니퓨리티 선물세트(7T*4종)", 2),
        _i(5, "닥터키토 방탄커피(번들/2개입)", 2),
        _i(6, "퍼펙토 낙산균(번들/2개입)", 2),
        _i(7, "퍼펙토 프리미엄 산양유(스틱)", 1),
        _i(8, "퍼펙토 발효 흑마늘", 1),
        _i(9, "퍼펙토 프리미엄 독일 맥주효모환(번들/2개입)", 1),
        _i(10, "스키니퓨리티 슈링티(30T)(번들/3개입)", 1),
        _i(11, "퍼펙토 프롤린 모유유산균(스틱)(번들/2개입)", 1),
        _i(12, "퍼펙토 프롤린 모유유산균(스틱)(번들/3개입)", 1),
        _i(13, "퍼펙토 시그니처 59 발효 효소(번들/2개입)", 1),
        _i(14, "스키니퓨리티 슈퍼프레쉬티(30T)", 1),
    ]
    r = assign_pallets(items, pallet_size=20)
    assert r.total_boxes == 67
    assert r.pallet_count == 4

    # 팔레트 1: SKU 1 단독 20박스
    assert len(r.pallets[0]) == 1
    assert r.pallets[0][0].key == 1
    assert r.pallets[0][0].boxes == 20

    # 팔레트 2: SKU 2 단독 20박스
    assert len(r.pallets[1]) == 1
    assert r.pallets[1][0].key == 2
    assert r.pallets[1][0].boxes == 20

    # 팔레트 3: 잔여풀에서 첫 묶음 = 20박스
    p3_total = sum(e.boxes for e in r.pallets[2])
    assert p3_total == 20

    # 팔레트 4: 나머지 7박스
    p4_total = sum(e.boxes for e in r.pallets[3])
    assert p4_total == 7


def test_pallet_no_of_lookup():
    items = [_i("A", "A", 24), _i("B", "B", 5)]
    r = assign_pallets(items, pallet_size=20)
    # A 는 두 팔레트에 분할 (20+4)
    assert sorted(r.pallet_no_of("A")) == [1, 2]
    # B 는 잔여풀과 함께 어딘가에
    assert len(r.pallet_no_of("B")) == 1


def test_skip_when_item_doesnt_fit_uses_smaller():
    """현재 used=18 인데 다음이 5박스라 안 들어감 → 1박스 SKU 찾아 채움."""
    items = [_i("a", "a", 18), _i("b", "b", 5), _i("c", "c", 1)]
    r = assign_pallets(items, pallet_size=20)
    # 팔레트 1: 18 + 1 = 19 (b=5는 skip)
    # 팔레트 2: 5
    assert r.pallet_count == 2
    p1_keys = sorted(e.key for e in r.pallets[0])
    assert p1_keys == ["a", "c"]
    assert sum(e.boxes for e in r.pallets[0]) == 19
    p2_keys = sorted(e.key for e in r.pallets[1])
    assert p2_keys == ["b"]


def test_multiple_full_then_leftover():
    """40박스 SKU 1개 → 단독 팔레트 2 + 작은 SKU 들 잔여풀."""
    items = [
        _i("A", "A", 40),
        _i("B", "B", 6),
        _i("C", "C", 5),
        _i("D", "D", 4),
    ]
    r = assign_pallets(items, pallet_size=20)
    # 팔레트 1, 2: A 단독 20씩
    assert r.pallets[0][0].key == "A"
    assert r.pallets[0][0].boxes == 20
    assert r.pallets[1][0].key == "A"
    # 팔레트 3: B(6)+C(5)+D(4) = 15
    assert r.pallet_count == 3
    p3 = r.pallets[2]
    assert sum(e.boxes for e in p3) == 15


def test_zero_boxes_excluded():
    items = [_i("A", "A", 0), _i("B", "B", 5)]
    r = assign_pallets(items, pallet_size=20)
    # A 는 0박스라 제외, B만
    assert r.pallet_count == 1
    assert r.pallets[0][0].key == "B"
