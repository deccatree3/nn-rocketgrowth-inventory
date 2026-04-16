"""설정 및 관리 페이지.

- 경고 임계값 조정 (현재 세션 전용, 실제 저장은 secrets.toml/DB에서)
- DB 상태 확인
- 제품 마스터 재이관 (템플릿 업로드)
"""
from __future__ import annotations

import streamlit as st
from sqlalchemy import func, select

from lib.config import load_config
from lib.db import get_session
from lib.models import (
    ActivityLog,
    CoupangInventorySnapshot,
    CoupangProduct,
    InboundPlan,
    WmsInventorySnapshot,
    WmsProduct,
)

st.set_page_config(page_title="설정", page_icon="⚙️", layout="wide")
st.title("⚙️ 설정 및 관리")

cfg = load_config()

st.subheader("경고 임계값 (현재 설정)")
c1, c2 = st.columns(2)
with c1:
    st.info(f"재고부족 임박: **{cfg.low_stock_days_threshold}일** 이하")
with c2:
    st.info(f"유통기한 임박 비율: **{cfg.near_expiry_ratio_threshold:.0%}** 이상")
st.caption(
    "임계값을 변경하려면 `.streamlit/secrets.toml` 의 `[app]` 섹션을 수정하세요. "
    "Streamlit Cloud 배포 환경에서는 App settings > Secrets."
)

st.divider()

st.subheader("DB 상태")
with get_session() as session:
    stats = {
        "wms_product": session.execute(select(func.count()).select_from(WmsProduct)).scalar() or 0,
        "coupang_product": session.execute(select(func.count()).select_from(CoupangProduct)).scalar() or 0,
        "coupang_inventory_snapshot": session.execute(
            select(func.count()).select_from(CoupangInventorySnapshot)
        ).scalar() or 0,
        "wms_inventory_snapshot": session.execute(
            select(func.count()).select_from(WmsInventorySnapshot)
        ).scalar() or 0,
        "inbound_plan": session.execute(select(func.count()).select_from(InboundPlan)).scalar() or 0,
        "activity_log": session.execute(select(func.count()).select_from(ActivityLog)).scalar() or 0,
    }
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("WMS 상품", stats["wms_product"])
c2.metric("쿠팡 상품", stats["coupang_product"])
c3.metric("쿠팡 스냅샷", stats["coupang_inventory_snapshot"])
c4.metric("WMS 스냅샷", stats["wms_inventory_snapshot"])
c5.metric("입고 계획", stats["inbound_plan"])
c6.metric("감사 로그", stats["activity_log"])

st.divider()

st.subheader("최근 활동 로그")
with get_session() as session:
    logs = session.execute(
        select(ActivityLog).order_by(ActivityLog.ts.desc()).limit(50)
    ).scalars().all()
if logs:
    st.dataframe(
        [
            {
                "시각": log.ts,
                "actor": log.actor,
                "action": log.action,
                "entity": log.entity,
                "entity_id": log.entity_id,
            }
            for log in logs
        ],
        use_container_width=True,
    )
else:
    st.caption("활동 기록 없음")

st.divider()

st.subheader("향후 기능 (중기)")
st.markdown(
    """
    - **쿠팡 셀러 API 자동 수집**: `lib/ingestion/coupang_api.py` 에 API 클라이언트를 구현하여
      매일 정기 스냅샷을 가져옵니다.
    - **GitHub Actions 스케줄러**: 매일 아침 재고 수집 + 엔진 실행 + 임박 SKU가 있으면 Slack 알림.
    - **Slack Webhook**: `lib/notifier/slack.py` — 재고부족, 회차 확정, 에러 알림.

    위 기능은 MVP 안정화 후 별도 단계에서 추가됩니다.
    """
)
