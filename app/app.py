"""메인 엔트리 — Streamlit 앱.

실행:
    cd app
    streamlit run app.py

Streamlit은 `pages/` 디렉토리 하위의 파일들을 자동으로 사이드바 페이지로 노출한다.
이 파일은 홈/대시보드 역할을 한다.
"""
from __future__ import annotations

import sys
import traceback

import streamlit as st

st.set_page_config(
    page_title="로켓그로스 입고 관리",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📦 로켓그로스 입고 관리")
st.caption("쿠팡 로켓그로스 밀크런 입고 계획 · 재고 · 이력")

# --- 진단 모드: import/DB 에러를 브라우저에 직접 표시 ---------------------
with st.expander("🛠 진단 정보 (배포 성공 시 접혀있음)", expanded=False):
    st.write(f"Python: `{sys.version}`")
    try:
        import pandas as pd  # noqa: F401
        import sqlalchemy as _sa
        st.write(f"pandas: `{pd.__version__}` · sqlalchemy: `{_sa.__version__}`")
    except Exception as e:
        st.error("기본 패키지 import 실패")
        st.exception(e)

try:
    from sqlalchemy import func, select

    from lib.db import get_session
    from lib.models import (
        CoupangInventorySnapshot,
        CoupangProduct,
        InboundPlan,
        WmsInventorySnapshot,
        WmsProduct,
    )
except Exception as e:
    st.error("🔴 모듈 import 실패 — 아래 트레이스 확인")
    st.exception(e)
    st.code("".join(traceback.format_exc()))
    st.stop()

# --- 개요 카드 ------------------------------------------------------------
try:
    with get_session() as session:
        wms_count = session.execute(select(func.count()).select_from(WmsProduct)).scalar() or 0
        cp_count = session.execute(select(func.count()).select_from(CoupangProduct)).scalar() or 0
        managed_count = (
            session.execute(
                select(func.count())
                .select_from(CoupangProduct)
                .where(CoupangProduct.milkrun_managed.is_(True))
            ).scalar()
            or 0
        )
        latest_coupang = session.execute(
            select(CoupangInventorySnapshot).order_by(CoupangInventorySnapshot.snapshot_date.desc()).limit(1)
        ).scalar_one_or_none()
        latest_wms = session.execute(
            select(WmsInventorySnapshot).order_by(WmsInventorySnapshot.snapshot_date.desc()).limit(1)
        ).scalar_one_or_none()
        plan_count = session.execute(select(func.count()).select_from(InboundPlan)).scalar() or 0
        latest_plan = session.execute(
            select(InboundPlan).order_by(InboundPlan.plan_date.desc()).limit(1)
        ).scalar_one_or_none()
except Exception as e:
    st.error("🔴 DB 연결/쿼리 실패 — 아래 트레이스 확인 (Supabase 연결 정보나 테이블 상태 점검 필요)")
    st.exception(e)
    st.code("".join(traceback.format_exc()))
    st.stop()

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric(
        "상품 정보 관리",
        f"WMS {wms_count} · 쿠팡 {cp_count}",
        delta=f"관리대상 {managed_count}",
    )
with c2:
    st.metric(
        "최신 쿠팡 재고 스냅샷",
        latest_coupang.snapshot_date.isoformat() if latest_coupang else "없음",
    )
with c3:
    st.metric(
        "최신 WMS 재고 스냅샷",
        latest_wms.snapshot_date.isoformat() if latest_wms else "없음",
    )
with c4:
    st.metric(
        "누적 입고 계획",
        f"{plan_count} (최근 {latest_plan.plan_date.isoformat() if latest_plan else '없음'})",
    )

st.divider()

st.subheader("빠른 시작")
st.markdown(
    """
    1. **상품 정보 관리** 페이지에서 박스낱수·유통기한·바코드 매핑을 점검/편집합니다.
    2. **입고계획 생성** 페이지에서 쿠팡 재고현황(xlsx) + WMS 재고현황(xls) + (선택) 쿠팡 업로드 양식을 업로드합니다.
    3. 자동 계산된 입고 수량을 검토·수정하고 저장합니다.
    4. 쿠팡 업로드 양식을 다운로드하여 쿠팡 Wing에 제출합니다.

    필요 시 **재고 현황** 페이지에서 경고 SKU를 확인하고, **이력 조회** 페이지에서 과거 회차를 비교할 수 있습니다.
    """
)

st.info(
    "향후 확장: 쿠팡 셀러 API 자동 수집 · Slack 알림 · 스케줄러 (GitHub Actions)",
    icon="🚀",
)
