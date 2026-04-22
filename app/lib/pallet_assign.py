"""팔레트 배분 알고리즘 — Split-First-then-Pack with Skip-Ahead.

규칙:
1) SKU 박스수 내림차순 정렬 (동률은 상품명 가나다 순)
2) 박스수 ≥ 팔레트 용량 인 SKU 는 단독 팔레트로 분할
   - 잔여(< 용량) 는 잔여풀로
3) 잔여풀에 대해 First-Fit Skip-Ahead:
   - 새 팔레트 시작
   - 잔여풀의 가장 큰 SKU 부터 시도, 들어가면 추가
   - 안 들어가면 다음(작은) SKU 시도 (skip-ahead)
   - 더 들어갈 SKU 가 없으면 팔레트 확정
4) 같은 SKU 분할 금지: 박스수 < 용량 SKU 는 통째로 한 팔레트에만

(`팔레트적재리스트` 같은 출력에서 1개 SKU 가 여러 행에 나뉘는 건
 박스수 ≥ 용량 분할 결과뿐이며, 잔여풀 분할은 발생하지 않는다.)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PalletItem:
    """입력 아이템."""

    key: Any           # 식별자 (예: coupang_option_id)
    name: str          # 상품명 (정렬·라벨용)
    boxes: int         # 박스수
    extras: dict[str, Any] = field(default_factory=dict)  # 부가 정보 통과용


@dataclass
class PalletEntry:
    """팔레트 안에 들어가는 하나의 SKU 행."""

    key: Any
    name: str
    boxes: int
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass
class PalletAssignment:
    pallets: list[list[PalletEntry]]   # 팔레트별 entry 리스트 (1-indexed 의미)
    total_boxes: int
    pallet_count: int

    def pallet_no_of(self, key: Any) -> list[int]:
        """key 의 SKU 가 속한 팔레트 번호(1-indexed) 들."""
        result = []
        for i, p in enumerate(self.pallets, start=1):
            if any(e.key == key for e in p):
                result.append(i)
        return result


def assign_pallets(items: list[PalletItem], pallet_size: int = 19) -> PalletAssignment:
    """팔레트 배분 수행.

    Args:
        items: 박스수 > 0 인 SKU 들
        pallet_size: 팔레트당 최대 박스수 (기본 19)

    Returns: PalletAssignment
    """
    if pallet_size <= 0:
        raise ValueError("pallet_size 는 양수여야 합니다")

    valid = [it for it in items if it.boxes and it.boxes > 0]
    if not valid:
        return PalletAssignment(pallets=[], total_boxes=0, pallet_count=0)

    # 1) 박스수 내림차순 정렬 (동률은 상품명)
    sorted_items = sorted(valid, key=lambda it: (-int(it.boxes), it.name or ""))

    pallets: list[list[PalletEntry]] = []
    leftover: list[PalletItem] = []

    # 2) 단독 팔레트 분할 (박스수 ≥ pallet_size)
    for it in sorted_items:
        boxes = int(it.boxes)
        if boxes >= pallet_size:
            full = boxes // pallet_size
            rem = boxes % pallet_size
            for _ in range(full):
                pallets.append(
                    [PalletEntry(key=it.key, name=it.name, boxes=pallet_size, extras=dict(it.extras))]
                )
            if rem > 0:
                leftover.append(
                    PalletItem(key=it.key, name=it.name, boxes=rem, extras=dict(it.extras))
                )
        else:
            leftover.append(PalletItem(key=it.key, name=it.name, boxes=boxes, extras=dict(it.extras)))

    # 3) 잔여풀 정렬 (박스수 내림차순, 동률 상품명)
    leftover.sort(key=lambda it: (-int(it.boxes), it.name or ""))

    # 4) Skip-Ahead First-Fit
    while leftover:
        current: list[PalletEntry] = []
        used = 0
        # 한 팔레트 채우기
        while True:
            picked_idx = None
            # 잔여풀이 박스수 내림차순이라 첫 번째 fit 이 가장 큰 fit
            for idx, it in enumerate(leftover):
                if used + it.boxes <= pallet_size:
                    picked_idx = idx
                    break
            if picked_idx is None:
                break
            it = leftover.pop(picked_idx)
            current.append(
                PalletEntry(key=it.key, name=it.name, boxes=it.boxes, extras=dict(it.extras))
            )
            used += it.boxes
        if current:
            pallets.append(current)
        else:
            # 안전 장치: 어떤 것도 못 들어가면 무한루프 방지
            break

    total_boxes = sum(e.boxes for p in pallets for e in p)
    return PalletAssignment(pallets=pallets, total_boxes=total_boxes, pallet_count=len(pallets))
