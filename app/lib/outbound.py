"""출고 배치 선택 + 부모 재고 풀 할당 로직.

핵심 규칙:
  1) 선입선출(FIFO): 유통기한이 짧은 배치를 먼저 고려
  2) 혼적 금지: 한 번의 출고는 **단일 배치**에서만 — 두 배치를 섞지 않음
  3) 부모 재고 공유: 번들(2개입/3개입 등)은 독립 재고가 없고, 부모(1개입)의 재고에서
     `unit_qty` 만큼 소모된다. 같은 부모를 공유하는 모든 출고는 **순차적으로 부모 풀을
     차감**하며, 남은 배치 상태에서 각자의 배치를 선택한다.

데이터 모델:
  Batch   = {"expiry": date|None, "available": int, "total": int}
  Pool    = {"parent_barcode": str, "batches": [Batch], "items": [Item]}
  Item    = {"key": any, "unit_qty": int, "requested_qty": int, ...}

함수:
  select_outbound_batch(qty, batches) → BatchSelection
      단일 아이템 + 단일 풀 기준. 할당 차감은 호출자가 수행.
  allocate_parent_pool(items, batches) → list[AllocationResult]
      풀 수준 순차 할당. 각 아이템의 소비 배치 + 상태를 결정하고
      배치의 remaining 을 가변적으로 차감한다.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass
class BatchSelection:
    status: str                 # 'ok' | 'no_demand' | 'no_batch' | 'insufficient'
    expiry: date | None         # 선택된 배치 유통일
    available: int              # 선택된 배치의 가용 수량
    requested_qty: int          # 요청 수량
    candidates: int             # 조회된 배치 수
    largest_available: int      # 가장 큰 단일 배치 수량 (insufficient 일 때 참고용)
    note: str | None = None


def select_outbound_batch(
    requested_qty: int,
    batches: list[dict[str, Any]] | None,
) -> BatchSelection:
    """배치 선택.

    Args:
        requested_qty: 출고해야 할 수량
        batches: [{"expiry": date, "available": int, ...}] — 유통일 오름차순 권장

    Returns:
        BatchSelection
    """
    if requested_qty is None or requested_qty <= 0:
        return BatchSelection(
            status="no_demand",
            expiry=None,
            available=0,
            requested_qty=requested_qty or 0,
            candidates=len(batches or []),
            largest_available=max((b.get("available", 0) for b in (batches or [])), default=0),
        )

    if not batches:
        return BatchSelection(
            status="no_batch",
            expiry=None,
            available=0,
            requested_qty=requested_qty,
            candidates=0,
            largest_available=0,
            note="WMS 재고 배치가 없습니다",
        )

    # 유통일 오름차순으로 정렬 (None 은 맨 뒤)
    sorted_batches = sorted(
        batches, key=lambda b: (b.get("expiry") is None, b.get("expiry"))
    )

    # 요청 수량을 단일 배치로 커버할 수 있는 가장 빠른 유통일 배치 선택
    for b in sorted_batches:
        if (b.get("available") or 0) >= requested_qty:
            return BatchSelection(
                status="ok",
                expiry=b.get("expiry"),
                available=int(b["available"]),
                requested_qty=requested_qty,
                candidates=len(sorted_batches),
                largest_available=int(b["available"]),
            )

    # 단일 배치 커버 불가
    largest = max((b.get("available") or 0 for b in sorted_batches), default=0)
    return BatchSelection(
        status="insufficient",
        expiry=None,
        available=0,
        requested_qty=requested_qty,
        candidates=len(sorted_batches),
        largest_available=int(largest),
        note=f"단일 배치로 {requested_qty}개 커버 불가 (최대 배치 {int(largest)}개)",
    )


# ============================================================================
# 부모 재고 풀 할당 (번들 포함)
# ============================================================================
@dataclass
class PoolAllocationItem:
    key: Any                       # 식별자 (coupang_option_id 등)
    unit_qty: int                  # 이 아이템 1개당 소비되는 부모 낱개수
    requested_qty: int             # 확정 출고 수량 (이 아이템 단위)


@dataclass
class PoolAllocationResult:
    key: Any
    unit_qty: int
    requested_qty: int
    base_units_needed: int         # = unit_qty * requested_qty
    status: str                    # 'ok' | 'no_demand' | 'no_batch' | 'insufficient'
    selected_batch_expiry: date | None
    selected_batch_total: int      # 선택된 배치의 원본 수량 (참고용)
    max_single_batch_after: int    # 이 아이템 확정 후 풀에 남은 최대 단일 배치
    pool_remaining_base_after: int # 이 아이템 확정 후 풀 잔여(낱개 기준)
    note: str | None = None


def allocate_parent_pool(
    items: list[PoolAllocationItem],
    batches: list[dict[str, Any]] | None,
) -> tuple[list[PoolAllocationResult], list[dict[str, Any]]]:
    """같은 부모 WMS 재고 풀을 공유하는 아이템들을 순차적으로 할당.

    각 아이템에 대해:
      - 필요 낱개수 = unit_qty * requested_qty
      - 풀의 배치 중 이 요구량을 단독으로 커버 가능한 가장 빠른 유통일 배치 선택
      - 선택된 배치의 remaining 에서 차감
      - 단독 커버 불가 시 insufficient 로 마킹 (차감 없음)

    아이템 순서가 우선순위 역할을 한다 (리스트 앞쪽이 우선).

    Returns:
        (results, updated_batches)
        updated_batches 는 입력 batches 의 deep-copy + remaining 차감본
    """
    updated = copy.deepcopy(batches or [])
    # 유통일 오름차순 정렬 (None 은 맨 뒤)
    updated.sort(key=lambda b: (b.get("expiry") is None, b.get("expiry")))
    # remaining 초기화
    for b in updated:
        b.setdefault("remaining", int(b.get("available") or 0))

    results: list[PoolAllocationResult] = []
    for item in items:
        need = int(item.unit_qty) * int(item.requested_qty)
        if item.requested_qty <= 0:
            pool_rem = sum(int(b["remaining"]) for b in updated)
            max_single = max((int(b["remaining"]) for b in updated), default=0)
            results.append(
                PoolAllocationResult(
                    key=item.key,
                    unit_qty=item.unit_qty,
                    requested_qty=item.requested_qty,
                    base_units_needed=0,
                    status="no_demand",
                    selected_batch_expiry=None,
                    selected_batch_total=0,
                    max_single_batch_after=max_single,
                    pool_remaining_base_after=pool_rem,
                )
            )
            continue

        if not updated:
            results.append(
                PoolAllocationResult(
                    key=item.key,
                    unit_qty=item.unit_qty,
                    requested_qty=item.requested_qty,
                    base_units_needed=need,
                    status="no_batch",
                    selected_batch_expiry=None,
                    selected_batch_total=0,
                    max_single_batch_after=0,
                    pool_remaining_base_after=0,
                    note="WMS 재고 배치가 없습니다",
                )
            )
            continue

        chosen = None
        for b in updated:
            if int(b["remaining"]) >= need:
                chosen = b
                break

        if chosen is not None:
            chosen["remaining"] = int(chosen["remaining"]) - need
            pool_rem = sum(int(b["remaining"]) for b in updated)
            max_single = max((int(b["remaining"]) for b in updated), default=0)
            results.append(
                PoolAllocationResult(
                    key=item.key,
                    unit_qty=item.unit_qty,
                    requested_qty=item.requested_qty,
                    base_units_needed=need,
                    status="ok",
                    selected_batch_expiry=chosen.get("expiry"),
                    selected_batch_total=int(chosen.get("total") or chosen.get("available") or 0),
                    max_single_batch_after=max_single,
                    pool_remaining_base_after=pool_rem,
                )
            )
        else:
            pool_rem = sum(int(b["remaining"]) for b in updated)
            max_single = max((int(b["remaining"]) for b in updated), default=0)
            results.append(
                PoolAllocationResult(
                    key=item.key,
                    unit_qty=item.unit_qty,
                    requested_qty=item.requested_qty,
                    base_units_needed=need,
                    status="insufficient",
                    selected_batch_expiry=None,
                    selected_batch_total=0,
                    max_single_batch_after=max_single,
                    pool_remaining_base_after=pool_rem,
                    note=f"필요 {need}낱개, 단일 배치 최대 {max_single}낱개",
                )
            )

    return results, updated
