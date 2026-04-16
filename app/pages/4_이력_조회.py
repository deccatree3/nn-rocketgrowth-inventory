"""과거 입고 이력 조회."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import select

from lib.db import get_session
from lib.models import InboundPlan, InboundPlanItem

st.set_page_config(page_title="이력 조회", page_icon="📚", layout="wide")
st.title("📚 입고 이력 조회")

with get_session() as session:
    plans = session.execute(
        select(InboundPlan).order_by(InboundPlan.plan_date.desc(), InboundPlan.id.desc())
    ).scalars().all()

if not plans:
    st.info("저장된 입고 계획이 아직 없습니다.")
    st.stop()

plans_df = pd.DataFrame(
    [
        {
            "id": p.id,
            "작업일": p.plan_date,
            "입고일": p.arrival_date,
            "FC": p.fc_name,
            "작업자": p.worker,
            "상태": p.status,
            "생성시각": p.created_at,
        }
        for p in plans
    ]
)
st.subheader("회차 목록")
st.dataframe(plans_df, use_container_width=True, height=300)

selected_id = st.selectbox("상세 조회할 회차 선택", options=[p.id for p in plans], format_func=lambda i: (
    f"#{i} - 작업 {plans_df[plans_df['id']==i].iloc[0]['작업일']} / 입고 {plans_df[plans_df['id']==i].iloc[0]['입고일']} · "
    f"{plans_df[plans_df['id']==i].iloc[0]['FC']} · {plans_df[plans_df['id']==i].iloc[0]['작업자']}"
))

if selected_id:
    with get_session() as session:
        items = session.execute(
            select(InboundPlanItem).where(InboundPlanItem.plan_id == selected_id)
        ).scalars().all()

    items_df = pd.DataFrame(
        [
            {
                "옵션ID": i.coupang_option_id,
                "상품명": i.product_name,
                "옵션": i.option_name,
                "현재재고": i.current_total_stock,
                "7일": i.sales_7d,
                "30일": i.sales_30d,
                "속도": float(i.sales_velocity_daily or 0),
                "4주후": float(i.stock_after_4w or 0),
                "박스낱수": i.box_qty,
                "추천입고": i.inbound_qty_suggested,
                "확정입고": i.inbound_qty_final,
                "박스수": i.inbound_boxes,
            }
            for i in items
        ]
    )
    st.subheader(f"회차 #{selected_id} 상세")
    st.dataframe(items_df[items_df["확정입고"] > 0], use_container_width=True, height=500)

# 제품별 추이
st.divider()
st.subheader("제품별 회차간 확정입고 추이")
with get_session() as session:
    all_items = session.execute(
        select(InboundPlan.plan_date, InboundPlanItem.coupang_option_id, InboundPlanItem.product_name, InboundPlanItem.inbound_qty_final)
        .join(InboundPlanItem, InboundPlan.id == InboundPlanItem.plan_id)
        .order_by(InboundPlan.plan_date)
    ).all()

if all_items:
    history_df = pd.DataFrame(
        all_items, columns=["plan_date", "option_id", "product_name", "qty"]
    )
    history_df = history_df[history_df["qty"] > 0]
    if len(history_df) > 0:
        options = history_df["product_name"].dropna().unique().tolist()
        selected_prod = st.selectbox("제품 선택", options=options)
        sub = history_df[history_df["product_name"] == selected_prod]
        fig = px.line(sub, x="plan_date", y="qty", markers=True, title=f"{selected_prod} 회차별 입고량")
        st.plotly_chart(fig, use_container_width=True)
