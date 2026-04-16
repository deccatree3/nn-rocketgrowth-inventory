"""팔레트 단위 입고수량 최적화.

목표:
    밀크런은 팔레트 단위 납품이므로, 전체 발주 박스 합계가 팔레트 크기(기본 20박스)의
    배수가 되도록 조정한다. 물류비 효율 극대화 목적.

핵심 알고리즘 — 2단계 "잠금 + 플렉서블 핏":

    [1] 보호 영역 (LOCKED): urgency in {critical, replenish}
        → 추천 수량 그대로, 조정 대상 X (결품 리스크 있는 SKU 감소 금지)

    [2] 조정 영역 (FLEXIBLE): urgency in {stable, overstock, idle}
        → 박스 +1/−1 씩 가감 가능

    [3] 팔레트 올림/내림 결정 (rounding="auto"):
        residue = total_boxes % pallet_size
        up_delta = pallet_size − residue
        if up_delta / pallet_size > rounddown_threshold (기본 0.5):
            mode = "down"  (올림폭이 팔레트 절반 이상이면 차라리 내림)
        else:
            mode = "up"

    [4] 박스 추가/제거 (round-robin):
        mode == "up":
            - 후보: flexible SKU 중 velocity>0 인 것
            - 우선순위: days_until_stockout 오름차순 (빨리 소진될 것부터)
            - 제약: (a) 추가 후 cover_days ≤ overstock_days, (b) 부모 풀 잔여 충분

        mode == "down":
            - 후보: flexible 중 현재 박스수 > 0 인 것
            - 우선순위: days_until_stockout 내림차순 (여유 있는 것부터 제거)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PalletItem:
    """팔레트 최적화 입력 아이템."""

    key: Any                               # 식별자 (coupang_option_id)
    urgency: str                           # critical|replenish|stable|overstock|idle|...
    basic_boxes: int                       # v2 엔진 추천 박스수
    box_qty: int                           # 1박스당 낱개수
    unit_qty: int                          # 번들 단위 (1,2,3...)
    parent_barcode: str | None             # 부모 WMS바코드 (풀 제약 체크용)
    current_total_stock: int               # 쿠팡 총재고 (orderable + inbound_stock)
    velocity: float                        # 판매속도 (개/일)
    days_until_stockout: float | None      # 현재 기준 소진 예상일


PROTECTED_URGENCIES = frozenset({"critical", "replenish"})
FLEXIBLE_UP_URGENCIES = frozenset({"stable", "overstock", "idle"})
FLEXIBLE_DOWN_URGENCIES = frozenset({"stable", "overstock"})  # idle 은 이미 0


@dataclass
class PalletResult:
    optimized_boxes: dict[Any, int]        # {key: 박스수}
    mode: str                              # "noop" | "up" | "down"
    requested_delta: int                   # 원래 필요했던 박스 수 (+/-)
    applied_delta: int                     # 실제 적용된 박스 수
    unfilled: int                          # 제약으로 못 채운 박스
    total_boxes_before: int
    total_boxes_after: int
    pallet_count: int
    adjustments: list[tuple[Any, int]] = field(default_factory=list)  # [(key, delta_boxes)]
    parent_pools_after: dict[str, int] = field(default_factory=dict)


def optimize_to_pallet(
    items: list[PalletItem],
    parent_pools_remaining: dict[str, int],
    *,
    pallet_size: int = 20,
    overstock_days: int = 35,
    rounding: str = "auto",  # "auto" | "up" | "down"
    rounddown_threshold: float = 0.5,
) -> PalletResult:
    """items 의 basic_boxes 를 출발점으로 팔레트 단위에 맞춘 박스수를 산출.

    parent_pools_remaining 은 호출 시점의 잔여 낱개수 (원본은 수정하지 않음).
    """
    pools = dict(parent_pools_remaining)
    optimized: dict[Any, int] = {it.key: int(it.basic_boxes) for it in items}
    adjustments: list[tuple[Any, int]] = []

    total_before = sum(optimized.values())
    if total_before == 0 or pallet_size <= 0:
        return PalletResult(
            optimized_boxes=optimized,
            mode="noop",
            requested_delta=0,
            applied_delta=0,
            unfilled=0,
            total_boxes_before=total_before,
            total_boxes_after=total_before,
            pallet_count=total_before // pallet_size if pallet_size > 0 else 0,
            adjustments=[],
            parent_pools_after=pools,
        )

    residue = total_before % pallet_size
    if residue == 0:
        return PalletResult(
            optimized_boxes=optimized,
            mode="noop",
            requested_delta=0,
            applied_delta=0,
            unfilled=0,
            total_boxes_before=total_before,
            total_boxes_after=total_before,
            pallet_count=total_before // pallet_size,
            adjustments=[],
            parent_pools_after=pools,
        )

    up_delta = pallet_size - residue
    down_delta = residue

    if rounding == "auto":
        # 올림폭이 팔레트 크기의 절반을 넘으면 차라리 내림
        mode = "down" if (up_delta / pallet_size > rounddown_threshold) else "up"
    elif rounding == "up":
        mode = "up"
    else:
        mode = "down"

    if mode == "up":
        applied = _apply_up(
            items, optimized, pools, up_delta, overstock_days, adjustments
        )
        unfilled = up_delta - applied
        total_after = total_before + applied
        requested = up_delta
    else:
        applied = _apply_down(items, optimized, pools, down_delta, adjustments)
        unfilled = down_delta - applied
        total_after = total_before - applied
        requested = -down_delta

    pallet_count = total_after // pallet_size + (1 if total_after % pallet_size else 0)

    return PalletResult(
        optimized_boxes=optimized,
        mode=mode,
        requested_delta=requested,
        applied_delta=applied if mode == "up" else -applied,
        unfilled=unfilled,
        total_boxes_before=total_before,
        total_boxes_after=total_after,
        pallet_count=pallet_count,
        adjustments=adjustments,
        parent_pools_after=pools,
    )


def _apply_up(
    items: list[PalletItem],
    optimized: dict[Any, int],
    pools: dict[str, int],
    delta: int,
    overstock_days: int,
    adjustments: list[tuple[Any, int]],
) -> int:
    """박스 +1씩 delta 번 추가. 실제 적용 박스수 반환."""
    # 후보: flexible, 판매속도 > 0
    candidates = [
        it for it in items
        if it.urgency not in PROTECTED_URGENCIES and it.velocity > 0
    ]
    # 우선순위: days_until_stockout 오름차순 (빨리 소진될 것부터)
    candidates.sort(key=lambda x: (x.days_until_stockout is None, x.days_until_stockout or 0))

    applied = 0
    # 각 반복마다 top 후보에 +1박스 시도. 실패(제약)하면 후보에서 제외 후 재시도.
    safety_iter = delta * 20 + 10
    while applied < delta and candidates and safety_iter > 0:
        safety_iter -= 1
        advanced = False
        i = 0
        while i < len(candidates) and applied < delta:
            it = candidates[i]
            if _can_add_box(it, optimized, pools, overstock_days):
                # 적용
                optimized[it.key] += 1
                if it.parent_barcode:
                    pools[it.parent_barcode] = pools.get(it.parent_barcode, 0) - it.box_qty * it.unit_qty
                adjustments.append((it.key, 1))
                applied += 1
                advanced = True
                # 재시도 위해 다음 후보로 round-robin
                i += 1
            else:
                # 이 SKU 는 더 이상 추가 불가 → 제거
                candidates.pop(i)
                advanced = True
        if not advanced:
            break
    return applied


def _apply_down(
    items: list[PalletItem],
    optimized: dict[Any, int],
    pools: dict[str, int],
    delta: int,
    adjustments: list[tuple[Any, int]],
) -> int:
    """박스 -1씩 delta 번 제거. 실제 적용 박스수 반환."""
    candidates = [
        it for it in items
        if it.urgency in FLEXIBLE_DOWN_URGENCIES and optimized[it.key] > 0
    ]
    # 우선순위: days_until_stockout 내림차순 (여유 있는 것부터)
    candidates.sort(
        key=lambda x: -(x.days_until_stockout or 0)
    )

    applied = 0
    safety_iter = delta * 20 + 10
    while applied < delta and candidates and safety_iter > 0:
        safety_iter -= 1
        advanced = False
        i = 0
        while i < len(candidates) and applied < delta:
            it = candidates[i]
            if optimized[it.key] > 0:
                optimized[it.key] -= 1
                if it.parent_barcode:
                    pools[it.parent_barcode] = pools.get(it.parent_barcode, 0) + it.box_qty * it.unit_qty
                adjustments.append((it.key, -1))
                applied += 1
                advanced = True
                if optimized[it.key] == 0:
                    candidates.pop(i)
                else:
                    i += 1
            else:
                candidates.pop(i)
                advanced = True
        if not advanced:
            break
    return applied


def _can_add_box(
    item: PalletItem,
    optimized: dict[Any, int],
    pools: dict[str, int],
    overstock_days: int,
) -> bool:
    """박스 1개 추가 가능 여부 — cover_days 상한 + 부모 풀 여유."""
    new_boxes = optimized[item.key] + 1
    new_qty = new_boxes * item.box_qty
    # cover_days 상한: (현재총재고 + 추가된 수량) / velocity ≤ overstock_days
    if item.velocity > 0:
        projected_cover = (item.current_total_stock + new_qty) / item.velocity
        if projected_cover > overstock_days:
            return False
    # 부모 풀 여유: box_qty * unit_qty 이상 있어야 함
    if item.parent_barcode:
        needed = item.box_qty * item.unit_qty
        if pools.get(item.parent_barcode, 0) < needed:
            return False
    return True
