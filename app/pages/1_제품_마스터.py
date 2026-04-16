"""제품 마스터 편집 — WMS상품 / 쿠팡상품 두 탭.

wms_product:
  WMS바코드 PK, 박스낱수, 중량, 유통기한일수, 부모바코드 등 (물리 속성)
coupang_product:
  쿠팡옵션ID PK, 등록상품명, 옵션명, 수동입고여부(milkrun_managed),
  WMS바코드 FK
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from lib.db import get_session
from lib.master_io import parse_master_file, upsert_coupang_records, upsert_wms_records
from lib.models import ActivityLog, CoupangProduct, WmsProduct


def _nullable_int(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _nullable_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None


def _nullable_bool(v, default=False):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return bool(v)


st.set_page_config(page_title="제품 마스터", page_icon="📋", layout="wide")
st.title("📋 제품 마스터")
st.caption(
    "WMS상품(물리 속성: 박스낱수/중량/유통기한일수)과 쿠팡상품(옵션/수동입고여부)은 "
    "별도로 관리되며 WMS바코드로 연결됩니다."
)

# =========================================================================
# 📤 파일 업로드로 일괄 추가/수정/교체
# =========================================================================
with st.expander("📤 파일 업로드로 일괄 관리 (추가/수정/교체)", expanded=False):
    st.markdown(
        "**마스터-상품정보.xlsx** 와 동일한 형식의 엑셀 파일을 업로드하세요.\n"
        "- **WMS상품정보** 시트: WMS바코드, 제품명, 낱개수량, 부모_WMS바코드, 1카톤박스입수량, 중량, 소비기한일수, 옵션ID, 부모_옵션ID\n"
        "- **쿠팡상품정보** 시트: 등록상품ID, 옵션ID, SKU ID, 등록상품명, 옵션명, 상품등급, 상품등록일, 수동입고여부, WMS바코드, 쿠팡바코드, WMS바코드-반품\n\n"
        "두 시트가 모두 있으면 양쪽 모두 적용, 한 시트만 있으면 해당 테이블만 적용."
    )

    col_up1, col_up2 = st.columns([3, 1])
    with col_up1:
        master_file = st.file_uploader(
            "마스터 파일 업로드 (.xlsx)",
            type=["xlsx"],
            key="master_upload",
        )
    with col_up2:
        upload_mode = st.radio(
            "적용 모드",
            ["추가/수정", "전체 교체"],
            index=0,
            key="upload_mode",
            help="**추가/수정**: 기존 데이터 유지 + 파일 내용 추가/수정. "
            "**전체 교체**: 파일에 없는 기존 항목 삭제. 파일이 마스터의 전체 원본이 됨.",
        )

    if master_file:
        try:
            parsed = parse_master_file(master_file.getvalue(), master_file.name)
            wms_count = len(parsed["wms"])
            cp_count = len(parsed["coupang"])
            st.success(f"파싱 완료: WMS {wms_count}건 · 쿠팡 {cp_count}건")

            if wms_count > 0:
                with st.popover(f"WMS {wms_count}건 미리보기"):
                    st.dataframe(pd.DataFrame(parsed["wms"]).head(20), use_container_width=True)
            if cp_count > 0:
                with st.popover(f"쿠팡 {cp_count}건 미리보기"):
                    st.dataframe(pd.DataFrame(parsed["coupang"]).head(20), use_container_width=True)

            replace_all = upload_mode == "전체 교체"
            if replace_all:
                st.warning(
                    "⚠️ 전체 교체 모드: 파일에 없는 기존 항목은 **삭제**됩니다. "
                    "파일이 마스터의 유일한 원본이 되어야 합니다."
                )

            if st.button("✅ DB에 적용", type="primary", key="apply_upload"):
                results = []
                if wms_count > 0:
                    s = upsert_wms_records(parsed["wms"], replace_all=replace_all)
                    results.append(f"WMS: +{s['added']} 추가, {s['updated']} 수정, -{s['deleted']} 삭제")
                if cp_count > 0:
                    s = upsert_coupang_records(parsed["coupang"], replace_all=replace_all)
                    results.append(f"쿠팡: +{s['added']} 추가, {s['updated']} 수정, -{s['deleted']} 삭제")
                st.success(" · ".join(results))
                st.cache_data.clear()
                st.rerun()
        except Exception as e:
            st.error(f"파일 처리 실패: {e}")

st.divider()

tab_wms, tab_cp = st.tabs(["🏭 WMS 상품", "🛒 쿠팡 상품"])


# =========================================================================
# WMS 상품 탭
# =========================================================================
# 컬럼명 매핑: DB → 마스터 파일 원본
WMS_COL_MAP = {
    "wms_barcode": "WMS바코드",
    "product_name": "제품명",
    "unit_qty": "낱개수량",
    "parent_wms_barcode": "부모_WMS바코드",
    "box_qty": "1카톤박스입수량",
    "weight_g": "중량",
    "shelf_life_days": "소비기한일수",
    "coupang_option_id": "옵션ID",
    "parent_coupang_option_id": "부모_옵션ID",
}
WMS_COL_REV = {v: k for k, v in WMS_COL_MAP.items()}

CP_COL_MAP = {
    "coupang_product_id": "등록상품 ID",
    "coupang_option_id": "옵션 ID",
    "sku_id": "SKU ID",
    "product_name": "등록상품명",
    "option_name": "옵션명",
    "grade": "상품등급",
    "registered_at": "상품등록일",
    "milkrun_managed": "수동입고여부",
    "wms_barcode": "WMS바코드",
    "coupang_barcode": "쿠팡바코드",
    "wms_barcode_return": "WMS바코드-반품",
    "active": "active",
}
CP_COL_REV = {v: k for k, v in CP_COL_MAP.items()}


@st.cache_data(ttl=60)
def load_wms() -> pd.DataFrame:
    with get_session() as s:
        rows = s.execute(select(WmsProduct).order_by(WmsProduct.wms_barcode)).scalars().all()
        df = pd.DataFrame(
            [
                {
                    "wms_barcode": r.wms_barcode,
                    "product_name": r.product_name,
                    "unit_qty": r.unit_qty,
                    "parent_wms_barcode": r.parent_wms_barcode,
                    "box_qty": r.box_qty,
                    "weight_g": r.weight_g,
                    "shelf_life_days": r.shelf_life_days,
                    "coupang_option_id": r.coupang_option_id,
                    "parent_coupang_option_id": r.parent_coupang_option_id,
                }
                for r in rows
            ]
        )
        return df.rename(columns=WMS_COL_MAP)


@st.cache_data(ttl=60)
def load_coupang() -> pd.DataFrame:
    with get_session() as s:
        rows = s.execute(select(CoupangProduct).order_by(CoupangProduct.coupang_option_id)).scalars().all()
        df = pd.DataFrame(
            [
                {
                    "coupang_product_id": r.coupang_product_id,
                    "coupang_option_id": r.coupang_option_id,
                    "sku_id": r.sku_id,
                    "product_name": r.product_name,
                    "option_name": r.option_name,
                    "grade": r.grade,
                    "registered_at": r.registered_at,
                    "milkrun_managed": r.milkrun_managed,
                    "wms_barcode": r.wms_barcode,
                    "coupang_barcode": r.coupang_barcode,
                    "wms_barcode_return": r.wms_barcode_return,
                    "active": r.active,
                }
                for r in rows
            ]
        )
        return df.rename(columns=CP_COL_MAP)


def persist_wms(original: pd.DataFrame, edited: pd.DataFrame) -> tuple[int, int, int]:
    # 컬럼명을 DB 이름으로 복원해서 처리
    original_db = original.rename(columns=WMS_COL_REV)
    edited_db = edited.rename(columns=WMS_COL_REV)
    original_ids = set(original_db["wms_barcode"].dropna().astype(str).tolist())
    edited_ids = set(edited_db["wms_barcode"].dropna().astype(str).tolist())
    deleted_ids = list(original_ids - edited_ids)

    saved = 0
    created = 0
    with get_session() as session:
        if deleted_ids:
            session.execute(delete(WmsProduct).where(WmsProduct.wms_barcode.in_(deleted_ids)))
            for bc in deleted_ids:
                session.add(
                    ActivityLog(actor="dashboard", action="delete", entity="wms_product", entity_id=bc)
                )

        for _, row in edited_db.iterrows():
            if pd.isna(row.get("wms_barcode")):
                continue
            rec = {
                "wms_barcode": str(row["wms_barcode"]).strip(),
                "product_name": _nullable_str(row.get("product_name")),
                "unit_qty": _nullable_int(row.get("unit_qty")),
                "parent_wms_barcode": _nullable_str(row.get("parent_wms_barcode")),
                "box_qty": _nullable_int(row.get("box_qty")),
                "weight_g": _nullable_int(row.get("weight_g")),
                "shelf_life_days": _nullable_int(row.get("shelf_life_days")),
                "coupang_option_id": _nullable_int(row.get("coupang_option_id")),
                "parent_coupang_option_id": _nullable_int(row.get("parent_coupang_option_id")),
            }
            stmt = pg_insert(WmsProduct).values(**rec)
            set_cols = {k: getattr(stmt.excluded, k) for k in rec if k != "wms_barcode"}
            set_cols["updated_at"] = datetime.now(timezone.utc)
            stmt = stmt.on_conflict_do_update(index_elements=["wms_barcode"], set_=set_cols)
            session.execute(stmt)
            if rec["wms_barcode"] not in original_ids:
                created += 1
            else:
                saved += 1
        session.commit()
    return saved, created, len(deleted_ids)


def persist_coupang(original: pd.DataFrame, edited: pd.DataFrame) -> tuple[int, int, int]:
    original_db = original.rename(columns=CP_COL_REV)
    edited_db = edited.rename(columns=CP_COL_REV)
    original_ids = set(original_db["coupang_option_id"].dropna().astype("int64").tolist())
    edited_ids = set(edited_db["coupang_option_id"].dropna().astype("int64").tolist())
    deleted_ids = list(original_ids - edited_ids)

    saved = 0
    created = 0
    with get_session() as session:
        if deleted_ids:
            session.execute(
                delete(CoupangProduct).where(CoupangProduct.coupang_option_id.in_(deleted_ids))
            )
            for oid in deleted_ids:
                session.add(
                    ActivityLog(
                        actor="dashboard",
                        action="delete",
                        entity="coupang_product",
                        entity_id=str(oid),
                    )
                )

        for _, row in edited_db.iterrows():
            if pd.isna(row.get("coupang_option_id")):
                continue
            rec = {
                "coupang_option_id": int(row["coupang_option_id"]),
                "coupang_product_id": _nullable_int(row.get("coupang_product_id")),
                "sku_id": _nullable_int(row.get("sku_id")),
                "product_name": str(row.get("product_name") or f"옵션 {int(row['coupang_option_id'])}"),
                "option_name": _nullable_str(row.get("option_name")),
                "grade": _nullable_str(row.get("grade")),
                "registered_at": row.get("registered_at") if not pd.isna(row.get("registered_at")) else None,
                "milkrun_managed": _nullable_bool(row.get("milkrun_managed"), False),
                "wms_barcode": _nullable_str(row.get("wms_barcode")),
                "coupang_barcode": _nullable_str(row.get("coupang_barcode")),
                "wms_barcode_return": _nullable_str(row.get("wms_barcode_return")),
                "active": _nullable_bool(row.get("active"), True),
            }
            stmt = pg_insert(CoupangProduct).values(**rec)
            set_cols = {k: getattr(stmt.excluded, k) for k in rec if k != "coupang_option_id"}
            set_cols["updated_at"] = datetime.now(timezone.utc)
            stmt = stmt.on_conflict_do_update(index_elements=["coupang_option_id"], set_=set_cols)
            session.execute(stmt)
            if rec["coupang_option_id"] not in original_ids:
                created += 1
            else:
                saved += 1
        session.commit()
    return saved, created, len(deleted_ids)


with tab_wms:
    wms_df = load_wms()
    search = st.text_input("🔍 바코드/제품명 검색", key="wms_search")

    view = wms_df.copy()
    if search:
        view = view[
            view["WMS바코드"].fillna("").str.contains(search, case=False)
            | view["제품명"].fillna("").str.contains(search, case=False)
        ]

    st.caption(f"표시: {len(view)} / 전체 {len(wms_df)}")

    edited_wms = st.data_editor(
        view,
        key="wms_editor",
        num_rows="dynamic",
        use_container_width=True,
        height=500,
    )
    if st.button("💾 WMS 저장", type="primary", key="save_wms"):
        try:
            s, c, d = persist_wms(wms_df, edited_wms)
            st.success(f"WMS: {s} 수정, {c} 신규, {d} 삭제")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"저장 실패: {e}")


with tab_cp:
    cp_df = load_coupang()
    search_cp = st.text_input("🔍 옵션ID/상품명 검색", key="cp_search")

    view = cp_df.copy()
    if search_cp:
        view = view[
            view["등록상품명"].fillna("").str.contains(search_cp, case=False)
            | view["옵션명"].fillna("").str.contains(search_cp, case=False)
            | view["옵션 ID"].astype(str).str.contains(search_cp)
        ]

    st.caption(f"표시: {len(view)} / 전체 {len(cp_df)}")

    edited_cp = st.data_editor(
        view,
        key="cp_editor",
        num_rows="dynamic",
        use_container_width=True,
        height=500,
    )
    if st.button("💾 쿠팡 저장", type="primary", key="save_cp"):
        try:
            s, c, d = persist_coupang(cp_df, edited_cp)
            st.success(f"쿠팡: {s} 수정, {c} 신규, {d} 삭제")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"저장 실패: {e}")
