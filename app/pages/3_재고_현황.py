"""재고 현황 및 경고 페이지.

최신 쿠팡/WMS 스냅샷 기준으로 전체 SKU의 재고 상태를 보여주고,
경고 대상(예상 소진일 임박, 유통기한 임박 비율) 을 하이라이트한다.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import select

from lib.config import load_config
from lib.db import get_session
from lib.models import (
    CoupangInventoryItem,
    CoupangInventorySnapshot,
    CoupangProduct,
    WmsInventoryItem,
    WmsInventorySnapshot,
)

st.set_page_config(page_title="재고 현황", page_icon="📊", layout="wide")
st.title("📊 재고 현황")

cfg = load_config()

with get_session() as session:
    latest_cp = session.execute(
        select(CoupangInventorySnapshot).order_by(CoupangInventorySnapshot.snapshot_date.desc()).limit(1)
    ).scalar_one_or_none()
    latest_wms = session.execute(
        select(WmsInventorySnapshot).order_by(WmsInventorySnapshot.snapshot_date.desc()).limit(1)
    ).scalar_one_or_none()

if not latest_cp:
    st.info("쿠팡 재고 스냅샷이 없습니다. 먼저 '입고계획 생성' 페이지에서 파일을 업로드하세요.")
    st.stop()

st.caption(
    f"쿠팡 스냅샷: {latest_cp.snapshot_date} · "
    f"WMS 스냅샷: {latest_wms.snapshot_date if latest_wms else '없음'}"
)

with get_session() as session:
    cp_items = session.execute(
        select(CoupangInventoryItem).where(CoupangInventoryItem.snapshot_id == latest_cp.id)
    ).scalars().all()
    masters = {m.coupang_option_id: m for m in session.execute(select(CoupangProduct)).scalars().all()}

    wms_rows = []
    if latest_wms:
        wms_rows = session.execute(
            select(WmsInventoryItem).where(WmsInventoryItem.snapshot_id == latest_wms.id)
        ).scalars().all()

# 바코드별 WMS 가용 재고 집계
wms_avail_by_bc: dict[str, int] = {}
wms_short_expiry_by_bc: dict[str, object] = {}
for w in wms_rows:
    if not w.barcode:
        continue
    wms_avail_by_bc[w.barcode] = wms_avail_by_bc.get(w.barcode, 0) + (w.available_qty or 0)
    if w.expiry_short and (
        w.barcode not in wms_short_expiry_by_bc or w.expiry_short < wms_short_expiry_by_bc[w.barcode]
    ):
        wms_short_expiry_by_bc[w.barcode] = w.expiry_short

records = []
for it in cp_items:
    m = masters.get(it.coupang_option_id)
    orderable = it.orderable_stock or 0
    inbound_s = it.inbound_stock or 0
    total = orderable + inbound_s
    s7 = it.sales_qty_7d or 0
    s30 = it.sales_qty_30d or 0
    velocity = max(s7 / 7, s30 / 30)
    days_left = (total / velocity) if velocity > 0 else None

    near_expiry_qty = (it.expiry_1_30 or 0) + (it.expiry_31_45 or 0)
    deep_stock = (
        (it.expiry_61_120 or 0)
        + (it.expiry_121_180 or 0)
        + (it.expiry_181_plus or 0)
    )
    total_expiry_bucket = near_expiry_qty + (it.expiry_46_60 or 0) + deep_stock
    near_expiry_ratio = (
        near_expiry_qty / total_expiry_bucket if total_expiry_bucket > 0 else 0
    )

    wms_bc = m.wms_barcode if m else None
    records.append(
        {
            "옵션ID": it.coupang_option_id,
            "상품명": it.product_name or (m.product_name if m else ""),
            "옵션명": it.option_name or (m.option_name if m else ""),
            "판매가능": orderable,
            "입고중": inbound_s,
            "총재고": total,
            "7일판매": s7,
            "30일판매": s30,
            "속도/일": round(velocity, 2),
            "예상소진일": round(days_left, 1) if days_left is not None else None,
            "WMS가용": wms_avail_by_bc.get(wms_bc) if wms_bc else None,
            "WMS소비기한": wms_short_expiry_by_bc.get(wms_bc) if wms_bc else None,
            "임박(1~45일)": near_expiry_qty,
            "임박비율": round(near_expiry_ratio, 2),
        }
    )

df = pd.DataFrame(records)

# 필터
c1, c2, c3 = st.columns([2, 1, 1])
with c1:
    search = st.text_input("🔍 상품명 검색")
with c2:
    days_thr = st.number_input("소진일 ≤", value=cfg.low_stock_days_threshold, step=1)
with c3:
    ratio_thr = st.number_input("임박 비율 ≥", value=cfg.near_expiry_ratio_threshold, step=0.05, format="%.2f")

view = df.copy()
if search:
    view = view[view["상품명"].fillna("").str.contains(search, case=False)]

tab1, tab2, tab3 = st.tabs(["전체", "🚨 재고부족 임박", "⚠️ 유통기한 임박"])

with tab1:
    st.dataframe(view, use_container_width=True, height=600)

with tab2:
    low = view[view["예상소진일"].notna() & (view["예상소진일"] <= days_thr)].sort_values("예상소진일")
    st.caption(f"{len(low)}개 SKU: 예상 소진일 ≤ {days_thr}일")
    st.dataframe(low, use_container_width=True, height=600)

with tab3:
    near = view[view["임박비율"] >= ratio_thr].sort_values("임박비율", ascending=False)
    st.caption(f"{len(near)}개 SKU: 1~45일 유통기한 재고 비율 ≥ {ratio_thr:.0%}")
    st.dataframe(near, use_container_width=True, height=600)
