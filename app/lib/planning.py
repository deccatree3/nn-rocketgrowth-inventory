"""입고 계산 엔진 v2.

파라미터 기반 reorder-up-to-target 모델.

핵심 아이디어:
    "입고가 FC에 도착하는 시점에 `target_cover_days` 만큼의 판매 가능 재고가
     있도록 발주한다. 리드타임 동안 이미 팔려나갈 분량은 미리 차감한다."

수식:
    velocity = α × (sales_7d / 7) + (1 − α) × (sales_30d / 30)
             ↑ 최근(7일) 가중 α + 장기(30일) 가중 (1−α)

    L = lead_time_days          # 주문 → FC 입고까지
    T = target_cover_days       # 도착 후 원하는 판매 가능 일수 (기본 28일=4주)

    stock_at_arrival      = max(0, current_total − velocity × L)
    target_at_arrival     = velocity × T
    raw_need              = target_at_arrival − stock_at_arrival
                          = velocity × (T + L) − current_total
    inbound_qty           = ceil(max(0, raw_need) / box_qty) × box_qty

Urgency 4단계:
    critical   : days_until_stockout < L         → 리드타임 내 소진, 긴급
    replenish  : < L + T                         → 도착 후 T일 못 버팀, 정상 보충
    stable     : L + T ≤ days_until_stockout ≤ overstock_days  → 안정
    overstock  : > overstock_days                → 과잉재고, 타 채널 우선 판매 권장

호환 목적의 보조 출력:
    stock_after_1w/2w/4w — 선형 투영 (14일, 28일, 30일 구분 위해 유지)
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class PlanParams:
    """엔진 파라미터. `AppConfig` 에서 주입."""

    lead_time_days: int = 7
    target_cover_days: int = 28
    velocity_alpha: float = 0.4
    overstock_days: int = 60


URGENCY_CRITICAL = "critical"   # 🚨
URGENCY_REPLENISH = "replenish"  # ⚠️
URGENCY_STABLE = "stable"       # ✅
URGENCY_OVERSTOCK = "overstock"  # ❄️
URGENCY_NO_VELOCITY = "idle"    # ⏸ (판매 없음)


@dataclass
class PlanInput:
    coupang_option_id: int
    product_name: str | None
    option_name: str | None
    orderable_stock: int            # 쿠팡 판매가능재고
    inbound_stock: int              # 쿠팡 입고중재고
    sales_qty_7d: int
    sales_qty_30d: int
    box_qty: int | None


@dataclass
class PlanOutput:
    coupang_option_id: int
    current_total_stock: int             # orderable + inbound_stock
    sales_velocity_daily: float          # 가중 평균
    velocity_7d: float                   # s7/7
    velocity_30d: float                  # s30/30
    # 선형 투영 (참고용)
    stock_after_1w: float
    stock_after_2w: float
    stock_after_4w: float                # 30일 기준
    # 핵심 (신규)
    stock_at_arrival: float              # 도착 시점 예상재고 (보충 전)
    target_at_arrival: float             # 도착 시점 목표재고
    target_cover_days: int               # T
    lead_time_days: int                  # L
    # 발주
    box_qty: int
    inbound_qty_suggested: int
    inbound_boxes: int
    days_sellable_after: float | None    # (inbound + stock_at_arrival) / velocity
    days_until_stockout: float | None    # current_total / velocity
    urgency: str                         # URGENCY_* 상수


def compute_plan(inp: PlanInput, params: PlanParams | None = None) -> PlanOutput:
    """단일 SKU 에 대해 발주 계획을 산출."""
    if params is None:
        params = PlanParams()

    current_total = int(inp.orderable_stock or 0) + int(inp.inbound_stock or 0)

    s7 = float(inp.sales_qty_7d or 0)
    s30 = float(inp.sales_qty_30d or 0)
    v7 = s7 / 7.0
    v30 = s30 / 30.0

    # 가중 평균 + 30일 평균 하한 (s7이 0이어도 s30 기반 판매 속도 보장)
    alpha = max(0.0, min(1.0, params.velocity_alpha))
    velocity = alpha * v7 + (1 - alpha) * v30

    L = int(params.lead_time_days)
    T = int(params.target_cover_days)

    stock_1w = current_total - velocity * 7
    stock_2w = current_total - velocity * 14
    stock_4w = current_total - velocity * 30

    stock_at_arrival = max(0.0, current_total - velocity * L)
    target_at_arrival = velocity * T
    raw_need = target_at_arrival - stock_at_arrival

    box_qty = int(inp.box_qty) if inp.box_qty and inp.box_qty > 0 else 1
    if raw_need > 0:
        boxes = math.ceil(raw_need / box_qty)
    else:
        boxes = 0
    inbound_qty = boxes * box_qty

    days_sellable_after: float | None
    days_until_stockout: float | None
    if velocity > 0:
        days_sellable_after = (inbound_qty + stock_at_arrival) / velocity
        days_until_stockout = current_total / velocity
    else:
        days_sellable_after = None
        days_until_stockout = None

    # Urgency 등급 결정
    if velocity <= 0:
        urgency = URGENCY_NO_VELOCITY
    elif days_until_stockout is not None and days_until_stockout < L:
        urgency = URGENCY_CRITICAL
    elif days_until_stockout is not None and days_until_stockout < L + T:
        urgency = URGENCY_REPLENISH
    elif days_until_stockout is not None and days_until_stockout > params.overstock_days:
        urgency = URGENCY_OVERSTOCK
    else:
        urgency = URGENCY_STABLE

    return PlanOutput(
        coupang_option_id=inp.coupang_option_id,
        current_total_stock=current_total,
        sales_velocity_daily=round(velocity, 4),
        velocity_7d=round(v7, 4),
        velocity_30d=round(v30, 4),
        stock_after_1w=round(stock_1w, 2),
        stock_after_2w=round(stock_2w, 2),
        stock_after_4w=round(stock_4w, 2),
        stock_at_arrival=round(stock_at_arrival, 2),
        target_at_arrival=round(target_at_arrival, 2),
        target_cover_days=T,
        lead_time_days=L,
        box_qty=box_qty,
        inbound_qty_suggested=inbound_qty,
        inbound_boxes=boxes,
        days_sellable_after=round(days_sellable_after, 2) if days_sellable_after is not None else None,
        days_until_stockout=round(days_until_stockout, 2) if days_until_stockout is not None else None,
        urgency=urgency,
    )


def compute_plan_batch(inputs: list[PlanInput], params: PlanParams | None = None) -> list[PlanOutput]:
    return [compute_plan(x, params) for x in inputs]


URGENCY_ICONS: dict[str, str] = {
    URGENCY_CRITICAL: "🚨",
    URGENCY_REPLENISH: "⚠️",
    URGENCY_STABLE: "✅",
    URGENCY_OVERSTOCK: "❄️",
    URGENCY_NO_VELOCITY: "⏸",
}
URGENCY_LABELS: dict[str, str] = {
    URGENCY_CRITICAL: "긴급",
    URGENCY_REPLENISH: "보충",
    URGENCY_STABLE: "안정",
    URGENCY_OVERSTOCK: "과잉",
    URGENCY_NO_VELOCITY: "무판매",
}


def urgency_badge(urgency: str) -> str:
    icon = URGENCY_ICONS.get(urgency, "")
    label = URGENCY_LABELS.get(urgency, urgency)
    return f"{icon} {label}".strip()
