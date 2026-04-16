"""입고 계산 엔진 v2 테스트.

엔진 로직:
    velocity = α × (s7/7) + (1−α) × (s30/30)
    stock_at_arrival = max(0, current − velocity × L)
    target_at_arrival = velocity × T
    inbound = ceil(max(0, target − stock_at_arrival) / box) × box
"""
from __future__ import annotations

import math

import pytest

from lib.planning import (
    URGENCY_CRITICAL,
    URGENCY_OVERSTOCK,
    URGENCY_REPLENISH,
    URGENCY_STABLE,
    URGENCY_NO_VELOCITY,
    PlanInput,
    PlanParams,
    compute_plan,
)


DEFAULT_PARAMS = PlanParams(
    lead_time_days=7,
    target_cover_days=28,
    velocity_alpha=0.4,
    overstock_days=60,
)


def _mk(s7, s30, current, inbound_stock=0, box=10):
    return PlanInput(
        coupang_option_id=1,
        product_name="TEST",
        option_name="",
        orderable_stock=current,
        inbound_stock=inbound_stock,
        sales_qty_7d=s7,
        sales_qty_30d=s30,
        box_qty=box,
    )


def test_velocity_weighted_average():
    """velocity = 0.4*(7/7) + 0.6*(30/30) = 1.0 (균일 판매)."""
    out = compute_plan(_mk(s7=7, s30=30, current=100), DEFAULT_PARAMS)
    assert math.isclose(out.sales_velocity_daily, 1.0, abs_tol=1e-6)
    assert out.velocity_7d == 1.0
    assert out.velocity_30d == 1.0


def test_velocity_recent_spike_dampened():
    """s7=21, s30=30 (최근 가속): velocity = 0.4*3 + 0.6*1 = 1.8
    기존 max(3,1) = 3 대비 40% 완화됨.
    """
    out = compute_plan(_mk(s7=21, s30=30, current=100), DEFAULT_PARAMS)
    assert math.isclose(out.sales_velocity_daily, 1.8, abs_tol=1e-6)


def test_inbound_exact_target():
    """velocity=1, current=0, L=7, T=28 → 도착시점 재고 0, 목표 28
    raw_need = 28, box=10 → ceil(28/10)*10 = 30.
    """
    out = compute_plan(_mk(s7=7, s30=30, current=0, box=10), DEFAULT_PARAMS)
    assert out.sales_velocity_daily == 1.0
    assert out.stock_at_arrival == 0.0  # max(0, 0 - 7)
    assert out.target_at_arrival == 28.0
    assert out.inbound_qty_suggested == 30
    assert out.inbound_boxes == 3


def test_inbound_zero_when_current_sufficient():
    """이미 35일치 재고 보유 → 발주 0."""
    # v=1, current=50. stock_at_arrival = 43. target = 28. need < 0 → 0
    out = compute_plan(_mk(s7=7, s30=30, current=50, box=10), DEFAULT_PARAMS)
    assert out.inbound_qty_suggested == 0
    assert out.inbound_boxes == 0


def test_inbound_partial_top_up():
    """v=2, current=40 → arrival=40-14=26, target=56, need=30, box=10 → 30."""
    out = compute_plan(_mk(s7=14, s30=60, current=40, box=10), DEFAULT_PARAMS)
    assert out.sales_velocity_daily == 2.0
    assert out.stock_at_arrival == 26.0
    assert out.target_at_arrival == 56.0
    assert out.inbound_qty_suggested == 30


def test_inbound_box_rounding_up():
    """v=1, current=10 → arrival=3, need=25, box=10 → ceil(2.5)*10 = 30."""
    out = compute_plan(_mk(s7=7, s30=30, current=10, box=10), DEFAULT_PARAMS)
    assert out.inbound_qty_suggested == 30
    assert out.inbound_boxes == 3


def test_velocity_zero_no_demand():
    """판매 없음 → velocity=0, inbound=0, urgency=idle."""
    out = compute_plan(_mk(s7=0, s30=0, current=100), DEFAULT_PARAMS)
    assert out.sales_velocity_daily == 0.0
    assert out.inbound_qty_suggested == 0
    assert out.urgency == URGENCY_NO_VELOCITY
    assert out.days_until_stockout is None


def test_urgency_critical_within_lead_time():
    """L=7일 이내 소진 → critical."""
    # v=5, current=20 → runway=4일 < 7 → critical
    out = compute_plan(_mk(s7=35, s30=150, current=20, box=10), DEFAULT_PARAMS)
    assert out.days_until_stockout == 4.0
    assert out.urgency == URGENCY_CRITICAL


def test_urgency_replenish_between_lead_and_target():
    """L+T 범위 내 소진 → replenish."""
    # v=1, current=20 → runway=20 < 35(L+T=7+28) → replenish
    out = compute_plan(_mk(s7=7, s30=30, current=20, box=10), DEFAULT_PARAMS)
    assert out.days_until_stockout == 20.0
    assert out.urgency == URGENCY_REPLENISH


def test_urgency_stable():
    """L+T 이상, overstock 이하 → stable."""
    # v=1, current=50 → runway=50, 35≤50≤60 → stable
    out = compute_plan(_mk(s7=7, s30=30, current=50, box=10), DEFAULT_PARAMS)
    assert out.days_until_stockout == 50.0
    assert out.urgency == URGENCY_STABLE


def test_urgency_overstock():
    """overstock_days 초과 → overstock 경고."""
    # v=1, current=100 → runway=100 > 60 → overstock
    out = compute_plan(_mk(s7=7, s30=30, current=100, box=10), DEFAULT_PARAMS)
    assert out.urgency == URGENCY_OVERSTOCK


def test_stock_projections_linear():
    """1주/2주/4주 후 재고는 velocity 기반 선형 투영."""
    out = compute_plan(_mk(s7=14, s30=60, current=100, box=10), DEFAULT_PARAMS)
    # velocity = 0.4*2 + 0.6*2 = 2
    assert out.stock_after_1w == 86.0  # 100 - 2*7
    assert out.stock_after_2w == 72.0  # 100 - 2*14
    assert out.stock_after_4w == 40.0  # 100 - 2*30


def test_current_total_includes_inbound_stock():
    """현재 총재고 = 판매가능 + 입고중."""
    out = compute_plan(_mk(s7=0, s30=0, current=50, inbound_stock=30), DEFAULT_PARAMS)
    assert out.current_total_stock == 80


def test_custom_params_shorter_cover():
    """target_cover_days=14 (2주 목표)로 줄이면 발주량 감소."""
    params = PlanParams(lead_time_days=7, target_cover_days=14, velocity_alpha=0.4, overstock_days=60)
    out = compute_plan(_mk(s7=7, s30=30, current=0, box=10), params)
    # v=1, arrival=0, target=14, need=14 → 20
    assert out.inbound_qty_suggested == 20


def test_days_sellable_after_inbound():
    """입고 후 판매 가능 일수 = (inbound + stock_at_arrival) / velocity."""
    out = compute_plan(_mk(s7=7, s30=30, current=20, box=10), DEFAULT_PARAMS)
    # v=1, arrival=max(0, 20-7)=13, inbound=ceil((28-13)/10)*10=20 → after=33일
    assert out.inbound_qty_suggested == 20
    assert out.days_sellable_after == 33.0
