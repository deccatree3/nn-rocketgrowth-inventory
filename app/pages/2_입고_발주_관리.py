"""입고 발주 관리 — 밀크런 핵심 화면.

탭 1: 입고 발주 (파일 업로드 → 발주 계획 → 임시저장 → 쿠팡 업로드 양식 생성)
탭 2: 검수 & 2차 결과물 (쿠팡 결과 PDF 업로드 → 검수 → 2차 결과물 다운로드 → 확정)
"""
from __future__ import annotations

import io
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import openpyxl
import pandas as pd
import streamlit as st
from sqlalchemy import and_, desc, select

from lib.config import load_config
from lib.coupang_result import (
    AttachmentMeta,
    InvoiceMeta,
    parse_attachment_doc,
    parse_barcode_labels,
    parse_invoice_doc,
)
from lib.db import get_session
from lib.export import (
    ExportItem,
    build_plain_xlsx,
    dates_from_batch,
    default_expiry_dates,
    fill_coupang_template,
)
from lib.ingestion.base import CoupangSnapshot, WmsSnapshot
from lib.ingestion.coupang_file import parse_coupang_inventory_file
from lib.ingestion.wms_file import aggregate_wms_by_barcode, parse_wms_inventory_file
from lib.models import (
    CoupangInventoryItem,
    CoupangInventorySnapshot,
    CoupangProduct,
    CoupangResultLog,
    InboundPlan,
    InboundPlanItem,
    WmsInventoryItem,
    WmsInventorySnapshot,
    WmsProduct,
)
from lib.pallet_assign import PalletItem as PA_PalletItem, assign_pallets as pa_assign_pallets
from lib.secondary_export import (
    SecondaryItem,
    build_consolidation_list,
    build_invoice_upload_form,
    build_order_form,
    build_pallet_loading_list,
    build_shipping_bulk_form,
    parse_order_search_file,
    update_inventory_movement,
    validate_order_search,
)
from lib.verification import PlannedSku, VerificationReport, verify
from lib.outbound import PoolAllocationItem, allocate_parent_pool
from lib.pallet import PalletItem, optimize_to_pallet
from lib.planning import URGENCY_ICONS, PlanInput, PlanParams, compute_plan, urgency_badge


# ---------------------------------------------------------------------------
# helpers (먼저 정의)
# ---------------------------------------------------------------------------
def _ni(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _resolve_parent_barcode(
    cp_master: CoupangProduct | None,
    wms_masters_by_bc: dict[str, WmsProduct],
) -> tuple[str | None, int]:
    """coupang 옵션 → (부모 WMS 바코드, unit_qty) 를 결정.

    - wms_product 테이블의 parent_wms_barcode 와 unit_qty 를 우선 사용
    - parent 가 0/None/self 면 '자기 자신이 부모' 로 간주 (단일팩)
    """
    if not cp_master or not cp_master.wms_barcode:
        return None, 1
    bc = cp_master.wms_barcode
    wp = wms_masters_by_bc.get(bc)
    if not wp:
        return bc, 1
    unit_qty = int(wp.unit_qty or 1)
    parent = wp.parent_wms_barcode
    if not parent or str(parent) in ("0", "") or parent == bc:
        return bc, unit_qty
    return str(parent), unit_qty


def _upsert_coupang_snapshot(session, snap: CoupangSnapshot) -> CoupangInventorySnapshot:
    existing = session.execute(
        select(CoupangInventorySnapshot).where(
            and_(
                CoupangInventorySnapshot.snapshot_date == snap.snapshot_date,
                CoupangInventorySnapshot.source_type == snap.source_type,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return existing
    row = CoupangInventorySnapshot(
        snapshot_date=snap.snapshot_date,
        source_type=snap.source_type,
        source_file=snap.source_file,
    )
    session.add(row)
    session.flush()
    for r in snap.rows:
        session.add(
            CoupangInventoryItem(
                snapshot_id=row.id,
                coupang_option_id=r.coupang_option_id,
                coupang_product_id=r.coupang_product_id,
                sku_id=r.sku_id,
                product_name=r.product_name,
                option_name=r.option_name,
                sales_qty_7d=r.sales_qty_7d,
                sales_qty_30d=r.sales_qty_30d,
                orderable_stock=r.orderable_stock,
                inbound_stock=r.inbound_stock,
                storage_fee_month=r.storage_fee_month,
                expiry_1_30=r.expiry_1_30,
                expiry_31_45=r.expiry_31_45,
                expiry_46_60=r.expiry_46_60,
                expiry_61_120=r.expiry_61_120,
                expiry_121_180=r.expiry_121_180,
                expiry_181_plus=r.expiry_181_plus,
                recommendation=r.recommendation,
                raw=r.raw,
            )
        )
    return row


def _upsert_wms_snapshot(session, snap: WmsSnapshot) -> WmsInventorySnapshot:
    existing = session.execute(
        select(WmsInventorySnapshot).where(WmsInventorySnapshot.snapshot_date == snap.snapshot_date)
    ).scalar_one_or_none()
    if existing:
        return existing
    row = WmsInventorySnapshot(snapshot_date=snap.snapshot_date, source_file=snap.source_file)
    session.add(row)
    session.flush()
    for r in snap.rows:
        session.add(
            WmsInventoryItem(
                snapshot_id=row.id,
                barcode=r.barcode,
                product_name=r.product_name,
                loc_group=r.loc_group,
                loc=r.loc,
                total_qty=r.total_qty,
                alloc_qty=r.alloc_qty,
                available_qty=r.available_qty,
                expiry_short=r.expiry_short,
                expiry_long=r.expiry_long,
                raw=r.raw,
            )
        )
    return row


def _save_plan(
    cp_snap: CoupangSnapshot,
    wms_snap: WmsSnapshot,
    full_df: pd.DataFrame,
    company_name: str = "서현",
    shipment_type: str = "milkrun",
    total_weight_kg: float | None = None,
    movement_blob: bytes | None = None,
    movement_filename: str | None = None,
) -> int:
    """draft 상태로 임시 저장. 작업일/FC/작업자/검수메타는 검수 단계에서 채움."""
    with get_session() as session:
        cp_row = _upsert_coupang_snapshot(session, cp_snap)
        wms_row = _upsert_wms_snapshot(session, wms_snap)
        session.flush()

        plan = InboundPlan(
            company_name=company_name,
            shipment_type=shipment_type,
            plan_date=date.today(),  # 임시. 검수에서 도착예정일 채움
            fc_name=None,
            worker=None,
            coupang_snapshot_id=cp_row.id,
            wms_snapshot_id=wms_row.id,
            status="draft",
            total_weight_kg=total_weight_kg,
            movement_template_blob=movement_blob,
            movement_template_filename=movement_filename,
        )
        session.add(plan)
        session.flush()

        for _, row in full_df.iterrows():
            final_qty = int(row["inbound_final"] or 0)
            box_qty = int(row["box_qty"] or 1)
            session.add(
                InboundPlanItem(
                    plan_id=plan.id,
                    coupang_option_id=int(row["coupang_option_id"]),
                    product_name=row["product_name"],
                    option_name=row.get("option_name"),
                    current_total_stock=int((row["orderable"] or 0) + (row["inbound_stock"] or 0)),
                    sales_7d=int(row["sales_7d"] or 0),
                    sales_30d=int(row["sales_30d"] or 0),
                    sales_velocity_daily=float(row["velocity"] or 0),
                    stock_after_1w=None,
                    stock_after_2w=None,
                    stock_after_4w=float(row["stock_4w"] or 0),
                    box_qty=box_qty,
                    inbound_qty_suggested=int(row.get("inbound_basic") or 0),
                    inbound_qty_final=final_qty,
                    inbound_boxes=final_qty // max(box_qty, 1),
                    days_sellable_after=(
                        float(row["days_sellable_after"]) if row["days_sellable_after"] is not None else None
                    ),
                    wms_short_expiry=row.get("selected_batch_expiry"),
                    wms_long_expiry=None,
                )
            )
        session.commit()
        return plan.id


# ---------------------------------------------------------------------------
# 페이지
# ---------------------------------------------------------------------------
st.set_page_config(page_title="입고 발주 관리", page_icon="📦", layout="wide")
st.title("📦 입고 발주 관리")

st.caption("밀크런 입고 대상 SKU의 발주 수량을 확정하고 부모 재고 풀을 관리합니다.")

cfg = load_config()

# 엔진 파라미터 로드 + 표시
plan_params = PlanParams(
    lead_time_days=cfg.lead_time_days,
    target_cover_days=cfg.target_cover_days,
    velocity_alpha=cfg.velocity_alpha,
    overstock_days=cfg.overstock_days,
)
st.info(
    f"📐 **계산 파라미터**: 리드타임 **{plan_params.lead_time_days}일** · "
    f"목표 커버 **{plan_params.target_cover_days}일** · "
    f"판매속도 블렌딩 α={plan_params.velocity_alpha:.2f} (7일 평균 가중) · "
    f"과잉 경고 {plan_params.overstock_days}일 초과",
    icon="ℹ️",
)

# --- 1. 파일 업로드 -------------------------------------------------------
st.subheader("1. 파일 업로드")

from lib.file_classifier import (
    FILE_TYPE_COUPANG, FILE_TYPE_WMS, FILE_TYPE_TEMPLATE, FILE_TYPE_MOVEMENT,
    FILE_TYPE_LABELS, CompanyFileGroup, classify_uploaded_files,
)

uploaded_files = st.file_uploader(
    "파일을 한번에 올려주세요 (여러 업체 가능)",
    type=["xlsx", "xls"],
    accept_multiple_files=True,
    key="multi_upload",
    help="업체별 4종: 쿠팡 재고현황 / WMS 재고현황 / 쿠팡 업로드양식 / 재고이동건\n\n"
    "여러 업체 파일을 섞어 올려도 내용으로 자동 분류합니다.",
)

if not uploaded_files:
    st.info("파일을 업로드하세요. 업체별 파일 4종을 한 번에 올릴 수 있습니다.")
    st.stop()

# 자동 분류
classified, company_groups = classify_uploaded_files(uploaded_files)

# 분류 결과 표시
if company_groups:
    st.success(f"**{len(company_groups)}개 업체** 감지: {', '.join(company_groups.keys())}")
    for comp, grp in company_groups.items():
        icons = []
        for ft in [FILE_TYPE_COUPANG, FILE_TYPE_WMS, FILE_TYPE_TEMPLATE, FILE_TYPE_MOVEMENT]:
            if ft in grp.files:
                icons.append(f"✅ {FILE_TYPE_LABELS[ft]}")
            else:
                icons.append(f"❌ {FILE_TYPE_LABELS[ft]}")
        st.caption(f"**{comp}**: " + " · ".join(icons))

# 미분류 파일 경고
unclassified = [cf for cf in classified if not cf.company]
if unclassified:
    st.warning(
        f"⚠️ {len(unclassified)}개 파일의 업체를 식별 못 했습니다: "
        + ", ".join(cf.file.name for cf in unclassified)
    )

# 업체 선택
if not company_groups:
    st.error("업체를 식별할 수 없습니다. 제품 마스터에 해당 업체의 상품이 등록되어 있는지 확인하세요.")
    st.stop()

if len(company_groups) == 1:
    selected_company = list(company_groups.keys())[0]
else:
    selected_company = st.selectbox(
        "발주 진행할 업체 선택",
        options=list(company_groups.keys()),
        key="company_select",
    )

grp = company_groups[selected_company]
coupang_file = grp.files.get(FILE_TYPE_COUPANG)
wms_file = grp.files.get(FILE_TYPE_WMS)
template_file = grp.files.get(FILE_TYPE_TEMPLATE)
movement_file = grp.files.get(FILE_TYPE_MOVEMENT)

if grp.missing_types:
    missing_labels = [FILE_TYPE_LABELS[ft] for ft in grp.missing_types]
    st.info(f"**{selected_company}** 미감지 파일: {', '.join(missing_labels)}")

if not (coupang_file and wms_file and template_file and movement_file):
    st.warning(f"**{selected_company}** 의 4개 파일이 모두 필요합니다.")
    st.stop()


# --- 2. 파싱 -------------------------------------------------------------
@st.cache_data(show_spinner="쿠팡 재고 파싱 중...")
def _parse_cp(data: bytes, name: str) -> CoupangSnapshot:
    tmp = Path("./_tmp_cp_" + name)
    tmp.write_bytes(data)
    try:
        return parse_coupang_inventory_file(tmp)
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


@st.cache_data(show_spinner="WMS 재고 파싱 중...")
def _parse_wms(data: bytes, name: str) -> WmsSnapshot:
    tmp = Path("./_tmp_wms_" + name)
    tmp.write_bytes(data)
    try:
        return parse_wms_inventory_file(tmp)
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


cp_snap = _parse_cp(coupang_file.getvalue(), coupang_file.name)
wms_snap = _parse_wms(wms_file.getvalue(), wms_file.name)
wms_agg = aggregate_wms_by_barcode(wms_snap)

st.success(
    f"파싱 완료: 쿠팡 {len(cp_snap.rows)}건 ({cp_snap.snapshot_date}) / "
    f"WMS {len(wms_snap.rows)}행 → {len(wms_agg)} 바코드 ({wms_snap.snapshot_date}) "
    f"— RELEASEAREA 제외"
)

# --- 3. 제품 마스터 로드 --------------------------------------------------
with get_session() as session:
    cp_masters = session.execute(
        select(CoupangProduct).where(CoupangProduct.company_name == selected_company)
    ).scalars().all()
    wms_masters = session.execute(
        select(WmsProduct).where(WmsProduct.company_name == selected_company)
    ).scalars().all()
cp_master_by_opt = {m.coupang_option_id: m for m in cp_masters}
wms_master_by_bc = {m.wms_barcode: m for m in wms_masters}

include_all = False  # 비관리 SKU 는 항상 제외 (수동입고여부=1 만 표시)

# --- 4. 기본 추천 수량 계산 (판매 기반) -----------------------------------
rows = []
for cp in cp_snap.rows:
    cm = cp_master_by_opt.get(cp.coupang_option_id)
    if not cm:
        if not include_all:
            continue
    else:
        if not cm.milkrun_managed and not include_all:
            continue

    parent_bc, unit_qty = _resolve_parent_barcode(cm, wms_master_by_bc) if cm else (None, 1)
    # box_qty / shelf_life 는 wms_product 에서 직접 조회 (옵션의 wms_barcode 기준)
    own_bc = cm.wms_barcode if cm else None
    own_wp = wms_master_by_bc.get(own_bc) if own_bc else None
    # 번들은 자신의 wms_product 가 없을 수 있음 → 부모 마스터 fallback
    parent_wp = wms_master_by_bc.get(parent_bc) if parent_bc else None
    box_qty = (own_wp.box_qty if own_wp and own_wp.box_qty else None) or (
        parent_wp.box_qty if parent_wp and parent_wp.box_qty else None
    ) or 1
    shelf_life = (own_wp.shelf_life_days if own_wp else None) or (
        parent_wp.shelf_life_days if parent_wp else None
    )
    # WMS 단위 중량 (g) — own 우선, 없으면 부모
    weight_g = (own_wp.weight_g if own_wp and own_wp.weight_g else None) or (
        parent_wp.weight_g if parent_wp and parent_wp.weight_g else None
    ) or 0

    engine_out = compute_plan(
        PlanInput(
            coupang_option_id=cp.coupang_option_id,
            product_name=cp.product_name,
            option_name=cp.option_name,
            orderable_stock=cp.orderable_stock,
            inbound_stock=cp.inbound_stock,
            sales_qty_7d=cp.sales_qty_7d,
            sales_qty_30d=cp.sales_qty_30d,
            box_qty=box_qty,
        ),
        plan_params,
    )

    # 상품명: WMS 제품명 우선, 없으면 부모 WMS 제품명, 최후에 쿠팡 상품명
    wms_product_name = (
        (own_wp.product_name if own_wp and own_wp.product_name else None)
        or (parent_wp.product_name if parent_wp and parent_wp.product_name else None)
        or cp.product_name
        or (cm.product_name if cm else "")
    )

    rows.append(
        {
            "urgency": urgency_badge(engine_out.urgency),
            "urgency_key": engine_out.urgency,
            "coupang_option_id": cp.coupang_option_id,
            "parent_wms_barcode": parent_bc,
            "own_wms_barcode": own_bc,
            "unit_qty": unit_qty,
            "product_name": wms_product_name,
            "orderable": cp.orderable_stock,
            "inbound_stock": cp.inbound_stock,
            "sales_7d": cp.sales_qty_7d,
            "sales_30d": cp.sales_qty_30d,
            "velocity": round(engine_out.sales_velocity_daily, 2),
            "days_until_stockout": engine_out.days_until_stockout,
            "stock_at_arrival": round(engine_out.stock_at_arrival, 1),
            "target_at_arrival": round(engine_out.target_at_arrival, 1),
            "stock_2w": round(engine_out.stock_after_2w, 1),
            "stock_4w": round(engine_out.stock_after_4w, 1),
            "box_qty": box_qty,
            "basic_boxes": engine_out.inbound_boxes,                 # 기본 추천 박스수
            "inbound_basic": engine_out.inbound_qty_suggested,       # 기본 추천 수량 (box_qty 단위)
            "inbound_pallet": engine_out.inbound_qty_suggested,      # 팔레트 추천 (뒤에서 덮어씀)
            "pallet_boxes": engine_out.inbound_boxes,                # 팔레트 추천 박스수
            "pallet_adjusted": False,                                # 팔레트 최적화로 조정됐는지
            "inbound_final": engine_out.inbound_qty_suggested,          # 기본값 = 권장입고수1
            "days_sellable_after": round(engine_out.days_sellable_after, 1) if engine_out.days_sellable_after else None,
            "shelf_life_days": shelf_life,
            "weight_g": weight_g,
            "master_missing": cm is None,
        }
    )

if not rows:
    st.warning("표시할 SKU가 없습니다. '전체 표시'를 체크하거나 제품 마스터를 확인하세요.")
    st.stop()

base_df = pd.DataFrame(rows)

# --- 4-2. 팔레트 최적화 (토글) ----------------------------------------------
pallet_on = st.checkbox(
    "🚛 팔레트 단위 최적화 (1팔레트 = 20박스)",
    value=True,
    help="보호 영역(긴급/보충)은 그대로 두고, 안정 영역 SKU의 박스수를 조정해 총 박스수가 20의 배수가 되도록 맞춥니다. auto 모드: 올림폭이 팔레트 절반 이하면 올림, 초과면 내림.",
)

if pallet_on:
    # 부모 풀 초기 잔여 (basic 반영 전 — 전체 가용 낱개)
    initial_pools: dict[str, int] = {}
    for bc, agg in wms_agg.items():
        total_avail = sum(b.get("available") or 0 for b in (agg.get("batches") or []))
        initial_pools[bc] = int(total_avail)
    # basic 추천이 이미 부모 풀을 소비한다고 가정하고 미리 차감
    for _, row in base_df.iterrows():
        pbc = row["parent_wms_barcode"]
        if not pbc:
            continue
        basic_base_units = int(row["basic_boxes"]) * int(row["box_qty"]) * int(row["unit_qty"])
        initial_pools[pbc] = max(0, initial_pools.get(pbc, 0) - basic_base_units)

    pallet_items = [
        PalletItem(
            key=int(row["coupang_option_id"]),
            urgency=row["urgency_key"],
            basic_boxes=int(row["basic_boxes"] or 0),
            box_qty=int(row["box_qty"] or 1),
            unit_qty=int(row["unit_qty"] or 1),
            parent_barcode=row["parent_wms_barcode"],
            current_total_stock=int((row["orderable"] or 0) + (row["inbound_stock"] or 0)),
            velocity=float(row["velocity"] or 0),
            days_until_stockout=row["days_until_stockout"],
        )
        for _, row in base_df.iterrows()
    ]
    pallet_result = optimize_to_pallet(
        pallet_items,
        initial_pools,
        pallet_size=20,
        overstock_days=cfg.overstock_days,
        rounding="auto",
    )
    # 결과 주입
    for i, row in base_df.iterrows():
        key = int(row["coupang_option_id"])
        opt_boxes = int(pallet_result.optimized_boxes.get(key, row["basic_boxes"] or 0))
        base_df.at[i, "pallet_boxes"] = opt_boxes
        base_df.at[i, "inbound_pallet"] = opt_boxes * int(row["box_qty"] or 1)
        base_df.at[i, "pallet_adjusted"] = opt_boxes != int(row["basic_boxes"] or 0)
else:
    pallet_result = None


# --- 5. 세션 상태 & 편집 UI -----------------------------------------------
editor_key = f"editor_{cp_snap.snapshot_date}_{wms_snap.snapshot_date}"

# 현재까지 사용자가 직접 입력한 확정입고 값을 session_state 에 보존
# (추천값은 자동 채움 없이 빈칸으로 시작 — 1~2주 운영 후 로직 신뢰도 평가 후 자동 채움 전환)
if "inbound_final_by_opt" not in st.session_state:
    st.session_state["inbound_final_by_opt"] = {}

# base_df 에 세션 값 주입 (사용자가 실제 입력한 값이 있을 때만)
for i, row in base_df.iterrows():
    opt = int(row["coupang_option_id"])
    if opt in st.session_state["inbound_final_by_opt"]:
        base_df.at[i, "inbound_final"] = st.session_state["inbound_final_by_opt"][opt]


# --- 6. 부모 풀 할당 수행 --------------------------------------------------
def _allocate(df: pd.DataFrame) -> pd.DataFrame:
    """부모 WMS바코드 그룹별로 순차 할당하고 결과 컬럼을 부착."""
    df = df.copy()
    df["selected_batch_expiry"] = None
    df["selected_status"] = None
    df["pool_total_base"] = None
    df["pool_remaining_base"] = None
    df["max_single_batch_after"] = None

    for parent_bc, group in df.groupby("parent_wms_barcode", sort=False, dropna=False):
        if not parent_bc:
            # 부모 정보 없음 → 상태만 마킹
            for idx in group.index:
                df.at[idx, "selected_status"] = "no_parent"
            continue
        agg = wms_agg.get(parent_bc)
        batches = (agg or {}).get("batches") or []
        total_base = sum(b.get("available") or 0 for b in batches)

        items = [
            PoolAllocationItem(
                key=int(row["coupang_option_id"]),
                unit_qty=int(row["unit_qty"] or 1),
                requested_qty=int(row["inbound_final"] or 0),
            )
            for _, row in group.iterrows()
        ]
        results, _updated = allocate_parent_pool(items, batches)
        result_by_key = {r.key: r for r in results}
        for idx, row in group.iterrows():
            r = result_by_key[int(row["coupang_option_id"])]
            df.at[idx, "selected_batch_expiry"] = r.selected_batch_expiry
            df.at[idx, "selected_status"] = r.status
            df.at[idx, "pool_total_base"] = total_base
            df.at[idx, "pool_remaining_base"] = r.pool_remaining_base_after
            df.at[idx, "max_single_batch_after"] = r.max_single_batch_after
    return df


allocated_df = _allocate(base_df)

# 확정박스수 = 확정입고 / 박스낱수 (자동 계산, None 이면 None)
def _calc_confirmed_boxes(r):
    v = r["inbound_final"]
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    box = max(int(r["box_qty"] or 1), 1)
    return int(int(v) // box)


allocated_df["confirmed_boxes"] = allocated_df.apply(_calc_confirmed_boxes, axis=1)

# --- 7. 재발주(재생산) 알림 ---------------------------------------------
# 알림 조건 (간이):
#   A. 추천입고 > 부모 풀 가용낱개  → 당장 이번 밀크런조차 불가
#   B. 밀크런 출고 후 잔여 낱개 < 부모 풀 합산 판매속도 × 재생산리드타임(28일)
#       = 재생산 리드타임 동안 버틸 수 없음
# 정확 계산이 아닌 "트리거" 성격 — 상세 분석은 별도 메뉴에서 수행 예정.
st.subheader("2. 발주 수량 확정")

reproduction_lead = cfg.reproduction_lead_days  # 기본 28일

# 부모 풀별 velocity 합계 (base 낱개 단위)
pool_velocity: dict[str, float] = {}
for _, r in allocated_df.iterrows():
    p = r["parent_wms_barcode"]
    if not p:
        continue
    pool_velocity[p] = pool_velocity.get(p, 0.0) + float(r["velocity"] or 0) * int(r["unit_qty"] or 1)

pool_stats = (
    allocated_df[allocated_df["parent_wms_barcode"].notna()]
    .groupby("parent_wms_barcode", sort=False)
    .agg(
        item_count=("coupang_option_id", "count"),
        allocated_base=(
            "inbound_final",
            lambda s: int((s.fillna(0) * allocated_df.loc[s.index, "unit_qty"]).sum()),
        ),
        pool_total=("pool_total_base", "first"),
        pool_remaining=("pool_remaining_base", "min"),
        first_product=("product_name", "first"),
    )
    .reset_index()
)
pool_stats["pool_velocity"] = pool_stats["parent_wms_barcode"].map(pool_velocity).fillna(0)
pool_stats["reproduction_demand"] = pool_stats["pool_velocity"] * reproduction_lead
pool_stats["shortfall"] = pool_stats["reproduction_demand"] - pool_stats["pool_remaining"]
pool_stats["needs_reproduction"] = (
    (pool_stats["shortfall"] > 0) | (pool_stats["allocated_base"] > pool_stats["pool_total"])
)

repro_list = pool_stats[pool_stats["needs_reproduction"]].sort_values("shortfall", ascending=False)

with st.expander(
    f"🏭 재발주 필요 알림 ({len(repro_list)}건)"
    + (f" · 재생산 리드타임 {reproduction_lead}일 기준" if len(repro_list) > 0 else ""),
    expanded=len(repro_list) > 0,
):
    st.caption(
        f"조건: 밀크런 출고 후 잔여 WMS 낱개재고가 '부모 풀 판매속도 × {reproduction_lead}일'에 못 미치면 재발주 필요. "
        "정확 계산은 아님 (쿠팡 외 채널 판매는 미포함) — 운영 트리거 용도."
    )
    if len(repro_list) == 0:
        st.caption("✅ 모든 부모 풀이 재생산 리드타임 동안 자력 운영 가능")
    else:
        display = repro_list[
            [
                "parent_wms_barcode",
                "first_product",
                "item_count",
                "pool_total",
                "allocated_base",
                "pool_remaining",
                "pool_velocity",
                "reproduction_demand",
                "shortfall",
            ]
        ].rename(
            columns={
                "parent_wms_barcode": "부모바코드",
                "first_product": "대표상품",
                "item_count": "아이템수",
                "pool_total": "풀전체낱개",
                "allocated_base": "이번할당낱개",
                "pool_remaining": "출고후잔여",
                "pool_velocity": "풀속도/일",
                "reproduction_demand": f"{reproduction_lead}일수요",
                "shortfall": "부족분",
            }
        )
        st.dataframe(display, use_container_width=True)
        st.warning(
            f"⚠️ {len(repro_list)}개 부모 풀이 재생산 리드타임({reproduction_lead}일) 동안 버티지 못합니다. "
            "생산/발주 담당자에게 알리거나 이번 밀크런 수량을 조정하세요."
        )


# --- 8. 편집 테이블 -------------------------------------------------------

# 재고(번들) 컬럼 추가 = 재고(낱개) / 낱개수 (view 생성 전에 계산)
allocated_df["pool_remaining_bundle"] = allocated_df.apply(
    lambda r: (
        int(int(r["pool_remaining_base"]) // max(int(r["unit_qty"] or 1), 1))
        if r["pool_remaining_base"] is not None and not (isinstance(r["pool_remaining_base"], float) and pd.isna(r["pool_remaining_base"]))
        else None
    ),
    axis=1,
)

col_f1, col_f2 = st.columns([2, 1])
with col_f1:
    search = st.text_input("🔍 상품명 검색")
with col_f2:
    status_options = ["🚨 긴급", "⚠️ 보충", "✅ 안정", "❄️ 과잉", "⏸ 무판매"]
    status_filter = st.multiselect(
        "상태 필터",
        options=status_options,
        default=["🚨 긴급", "⚠️ 보충"],
        help="선택한 상태의 SKU만 표시합니다.",
    )

view = allocated_df.copy()
if search:
    view = view[view["product_name"].fillna("").str.contains(search, case=False, regex=False)]
if status_filter:
    view = view[view["urgency"].isin(status_filter)]

st.caption(
    f"표시: {len(view)} / 전체 {len(allocated_df)} · "
    f"확정수량을 편집하면 같은 부모 풀의 다른 아이템 '가능수량'이 재계산됩니다."
)

display_cols = [
    "coupang_option_id",  # 내부 키 (숨김)
    "urgency",            # 상태 — 맨 앞 (pinned)
    "product_name",       # 상품명 (pinned)
    "orderable",
    "sales_7d",
    "sales_30d",
    "velocity",
    "days_until_stockout",
    "box_qty",
    "inbound_basic",
    "basic_boxes",
    "pool_remaining_base",      # 재고(낱개) — 확정 앞
    "pool_remaining_bundle",    # 재고(번들) — 확정 앞
    "inbound_final",            # 확정 (강조)
    "confirmed_boxes",
    "selected_batch_expiry",
    "selected_status",          # 숨김 — 스타일링에 사용
]

# 색상 정의
DEFAULT_CONFIRM_BG = "#fff8d6"  # 옅은 노랑 — 입력 컬럼 강조
OVER_CONFIRM_BG = "background-color: #ff6b6b; color: white; font-weight: bold;"
OVER_STOCK_BG = "background-color: #ffe5e5;"


def _highlight_over(row):
    """재고 over 시 빨강 덮어쓰기. 정상이면 빈 스타일 (set_properties 의 노랑이 보존됨)."""
    styles = [""] * len(row)
    pool_rem = row.get("pool_remaining_base")
    status = row.get("selected_status")
    is_over = (
        (pool_rem is not None and not (isinstance(pool_rem, float) and pd.isna(pool_rem)) and pool_rem < 0)
        or status == "insufficient"
    )
    if is_over:
        cols = list(row.index)
        if "inbound_final" in cols:
            styles[cols.index("inbound_final")] = OVER_CONFIRM_BG
        for col in ("pool_remaining_base", "pool_remaining_bundle"):
            if col in cols:
                styles[cols.index(col)] = OVER_STOCK_BG
    return styles


# set_properties → apply 순서: 기본 노랑 깔고, over 행만 빨강으로 덮어쓰기
view_styled = (
    view[display_cols]
    .style.set_properties(subset=["inbound_final"], **{"background-color": DEFAULT_CONFIRM_BG})
    .apply(_highlight_over, axis=1)
)

edited = st.data_editor(
    view_styled,
    key=editor_key,
    use_container_width=True,
    height=500,
    hide_index=True,
    disabled=[c for c in display_cols if c != "inbound_final"],
    column_config={
        "urgency": st.column_config.TextColumn(
            "상태",
            help="🚨 긴급 · ⚠️ 보충 · ✅ 안정 · ❄️ 과잉 · ⏸ 무판매",
            width="small",
            pinned=True,
        ),
        "coupang_option_id": None,  # 숨김 (내부 키로만 사용)
        "product_name": st.column_config.TextColumn("상품명", width="large", pinned=True),
        "orderable": st.column_config.NumberColumn("쿠팡가용", format="%d"),
        "sales_7d": st.column_config.NumberColumn("7일", format="%d"),
        "sales_30d": st.column_config.NumberColumn("30일", format="%d"),
        "velocity": st.column_config.NumberColumn(
            "속도/일",
            format="%.2f",
            help=f"판매 속도 = α×(7일/7) + (1−α)×(30일/30), α={plan_params.velocity_alpha}",
        ),
        "days_until_stockout": st.column_config.NumberColumn(
            "소진예상(일)",
            format="%.1f",
            help="현재 재고가 velocity 기준 며칠 버티는지",
        ),
        "box_qty": st.column_config.NumberColumn("box입인", format="%d"),
        "inbound_basic": st.column_config.NumberColumn(
            "입고권장",
            format="%d",
            help="엔진 추천 낱개 수량 (팔레트 미고려)",
        ),
        "basic_boxes": st.column_config.NumberColumn(
            "입고권장(box)",
            format="%d",
            help="입고권장 ÷ box입인",
        ),
        "inbound_final": st.column_config.NumberColumn(
            "확정",
            format="%d",
            required=False,
            help="사용자가 직접 입력. 권장입고수1을 참고하여 결정",
        ),
        "confirmed_boxes": st.column_config.NumberColumn(
            "확정(box)",
            format="%d",
            help="확정 ÷ box입인 (자동)",
        ),
        "selected_batch_expiry": st.column_config.DateColumn(
            "소비기한",
            help="FIFO+혼적금지 규칙으로 선택된 출고 배치의 유통(소비)기한",
        ),
        "selected_status": None,  # 숨김 (스타일링용)
        "pool_remaining_base": st.column_config.NumberColumn(
            "재고(낱개)",
            format="%d",
            help="확정 후 같은 부모 풀에 남은 WMS 낱개 수",
        ),
        "pool_remaining_bundle": st.column_config.NumberColumn(
            "재고(번들)",
            format="%d",
            help="재고(낱개) ÷ 낱개수 — 이 SKU 기준으로 남은 번들 단위 수량",
        ),
    },
)

# 편집본을 session_state 에 반영 (다음 rerun 때 allocation 재계산)
changed = False
for _, erow in edited.iterrows():
    opt = int(erow["coupang_option_id"])
    raw_val = erow.get("inbound_final")
    if raw_val is None or (isinstance(raw_val, float) and pd.isna(raw_val)):
        # 사용자가 값을 지웠거나 아직 입력 안함
        if opt in st.session_state["inbound_final_by_opt"]:
            del st.session_state["inbound_final_by_opt"][opt]
            changed = True
    else:
        new_val = _ni(raw_val) or 0
        if st.session_state["inbound_final_by_opt"].get(opt) != new_val:
            st.session_state["inbound_final_by_opt"][opt] = new_val
            changed = True

if changed:
    st.rerun()

# 경고 배너
insufficient = allocated_df[allocated_df["selected_status"] == "insufficient"]
no_parent = allocated_df[allocated_df["selected_status"] == "no_parent"]
if len(insufficient) > 0:
    st.warning(
        f"⚠️ {len(insufficient)}개 SKU: 단일 배치로 확정수량 커버 불가. "
        f"수량을 줄이거나 출고를 분할 처리하세요."
    )
if len(no_parent) > 0:
    st.info(f"ℹ️ {len(no_parent)}개 SKU: 부모 WMS 바코드 매핑 없음. 제품 마스터에서 연결하세요.")

# --- 9. 요약 & 액션 -------------------------------------------------------
# 확정 수량 기반
confirmed_qty = int(edited["inbound_final"].fillna(0).sum())
confirmed_boxes_sum = 0
active_cnt = 0
total_weight_g = 0  # 총중량 (g)
for _, r in edited.iterrows():
    qty_raw = r.get("inbound_final")
    qty = 0
    if qty_raw is not None and not (isinstance(qty_raw, float) and pd.isna(qty_raw)):
        qty = int(qty_raw)
    if qty > 0:
        active_cnt += 1
        box = int(r.get("box_qty") or 1)
        boxes = qty // max(box, 1)
        confirmed_boxes_sum += boxes
        # 중량 계산: weight_g 는 allocated_df 에서 가져오기 (edited 에는 없을 수 있음)
        opt_id = int(r["coupang_option_id"])
        ar = allocated_df[allocated_df["coupang_option_id"] == opt_id]
        unit_w = int(ar.iloc[0]["weight_g"] or 0) if len(ar) > 0 else 0
        # (단위중량 × 확정수량 + 500 × 박스수)
        total_weight_g += unit_w * qty + 500 * boxes

total_weight_kg = total_weight_g / 1000

confirmed_pallets_float = confirmed_boxes_sum / 20 if confirmed_boxes_sum else 0

# 기본/팔레트 추천 합계 (비교용)
basic_boxes_sum = int(allocated_df["basic_boxes"].fillna(0).sum())
pallet_boxes_sum = int(allocated_df["pallet_boxes"].fillna(0).sum())

st.markdown("### 📊 요약")
col_s1, col_s2, col_s3, col_s4, col_s5 = st.columns(5)
with col_s1:
    st.metric("확정 수량 (낱개)", f"{confirmed_qty:,}")
with col_s2:
    st.metric("확정 박스수", f"{confirmed_boxes_sum:,}")
with col_s3:
    pallet_full = confirmed_boxes_sum // 20
    pallet_remainder = confirmed_boxes_sum % 20
    st.metric(
        "팔레트",
        f"{pallet_full}" + (f" + {pallet_remainder}박스" if pallet_remainder else " (꽉참)"),
    )
with col_s4:
    st.metric(
        "총중량 (kg)",
        f"{total_weight_kg:,.1f}",
        help="(WMS 단위중량 × 확정수량 + 500g × 박스수) ÷ 1000",
    )
with col_s5:
    st.metric("대상 SKU", f"{active_cnt}")

# 팔레트 최적화 상세
if pallet_on and pallet_result is not None and pallet_result.mode != "noop":
    with st.expander(
        f"🎯 팔레트 최적화 결과 ({pallet_result.mode} 모드, "
        f"{pallet_result.applied_delta:+d}박스, "
        f"{pallet_result.total_boxes_before}→{pallet_result.total_boxes_after})",
        expanded=False,
    ):
        if pallet_result.unfilled > 0:
            st.warning(
                f"⚠️ 제약(부모 풀 여유 / cover days 상한)으로 {pallet_result.unfilled} 박스를 더 채우지 못했습니다."
            )
        if pallet_result.adjustments:
            # 조정된 SKU 목록 (key, +/-)
            adj_map: dict[int, int] = {}
            for k, d in pallet_result.adjustments:
                adj_map[k] = adj_map.get(k, 0) + d
            adj_df = pd.DataFrame(
                [{"옵션ID": k, "박스 조정": v} for k, v in adj_map.items()]
            )
            adj_df = adj_df.merge(
                allocated_df[["coupang_option_id", "product_name"]],
                left_on="옵션ID",
                right_on="coupang_option_id",
                how="left",
            )[["옵션ID", "product_name", "박스 조정"]]
            adj_df.columns = ["옵션ID", "상품명", "박스 조정"]
            st.dataframe(adj_df, use_container_width=True, hide_index=True)
        else:
            st.caption("조정 없음")

st.divider()
st.subheader("3. 저장 및 쿠팡 업로드 파일 생성")

# export_items 준비 — allocated_df 전체 사용 (필터와 무관)
# edited (필터된 view) 에서 수정된 inbound_final 을 allocated_df 에 반영
_export_df = allocated_df.copy()
for _, erow in edited.iterrows():
    _eid = int(erow["coupang_option_id"])
    _ev = _ni(erow.get("inbound_final")) or 0
    _export_df.loc[_export_df["coupang_option_id"] == _eid, "inbound_final"] = _ev

export_items = []
for _, row in _export_df.iterrows():
    qty = _ni(row["inbound_final"]) or 0
    if qty <= 0:
        continue
    opt_id = int(row["coupang_option_id"])
    arow = allocated_df[allocated_df["coupang_option_id"] == opt_id]
    be = arow.iloc[0].get("selected_batch_expiry") if len(arow) > 0 else None
    slm = (
        int(arow.iloc[0]["shelf_life_days"])
        if len(arow) > 0 and pd.notna(arow.iloc[0].get("shelf_life_days"))
        else None
    )
    if be is not None and not (isinstance(be, float) and pd.isna(be)):
        exp, man = dates_from_batch(be, slm)
    else:
        exp, man = default_expiry_dates(slm)
    own_bc = arow.iloc[0]["own_wms_barcode"] if len(arow) > 0 else None
    export_items.append(
        ExportItem(
            coupang_option_id=opt_id,
            inbound_qty=qty,
            shelf_life_days=slm,
            expiry_date=exp,
            manufacture_date=man,
            wms_barcode=own_bc,
            product_name=row["product_name"],
        )
    )

# 쿠팡 양식: 필수 업로드 (template_file 은 이미 필수 검증 통과)
tpl_source = io.BytesIO(template_file.getvalue())
tpl_label = template_file.name

col_sv, col_dl = st.columns([1, 1])
with col_sv:
    if st.button("💾 임시 저장 (검수 대기)", type="primary", use_container_width=True):
        try:
            save_df = allocated_df.copy()
            for _, erow in edited.iterrows():
                opt = int(erow["coupang_option_id"])
                mask = save_df["coupang_option_id"] == opt
                save_df.loc[mask, "inbound_final"] = _ni(erow["inbound_final"]) or 0
            plan_id = _save_plan(
                cp_snap=cp_snap,
                wms_snap=wms_snap,
                full_df=save_df,
                company_name=cfg.default_company_name,
                shipment_type=cfg.default_shipment_type,
                total_weight_kg=total_weight_kg,
                movement_blob=movement_file.getvalue() if movement_file else None,
                movement_filename=movement_file.name if movement_file else None,
            )
            st.success(
                f"임시 저장 완료 (plan_id={plan_id}, status=draft). "
                f"쿠팡 어드민에 업로드 후 '검수·2차결과물' 페이지에서 검수해주세요."
            )
        except Exception as e:
            st.error(f"저장 실패: {e}")

with col_dl:
    if confirmed_qty == 0:
        st.button("📥 쿠팡 업로드 파일 생성", disabled=True, use_container_width=True)
        st.caption("확정 수량을 입력하세요.")
    else:
        try:
            xlsx, _missing = fill_coupang_template(
                tpl_source,
                export_items,
                delete_non_target=True,
            )
            st.download_button(
                "📥 쿠팡 업로드 파일 생성",
                data=xlsx,
                file_name=f"generated_excel_{date.today().isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )
            filled_count = len(export_items) - len(_missing)
            st.caption(
                f"✅ {filled_count}/{len(export_items)}건 반영 · 양식: {tpl_label} · "
                f"입고수량·유통기한·제조일자 채움, 비대상 행 삭제"
            )
            if _missing:
                st.error(
                    f"⚠️ {len(_missing)}건 누락: 쿠팡 양식에 해당 옵션ID가 없어 반영되지 않았습니다. "
                    f"쿠팡 Wing에서 최신 양식을 다시 다운받아 사용하세요."
                )
                st.dataframe(
                    pd.DataFrame(_missing).rename(columns={
                        "coupang_option_id": "옵션ID",
                        "product_name": "상품명",
                        "inbound_qty": "확정수량",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
        except Exception as e:
            st.error(f"양식 생성 실패: {e}")


# ############################################################################
# 4. 검수 & 2차 결과물 생성
# ############################################################################
st.divider()
st.header("📑 검수 & 2차 결과물 생성")
st.caption("쿠팡 어드민 결과물(PDF)을 업로드해 검수하고 2차 결과물을 생성합니다.")

# --- Draft 발주 선택 ---
st.subheader("4-1. 임시저장된 발주 선택")

with get_session() as _sess:
    _drafts = (
        _sess.execute(
            select(InboundPlan)
            .where(InboundPlan.status == "draft")
            .order_by(desc(InboundPlan.created_at))
        )
        .scalars()
        .all()
    )
    _draft_data = [
        {
            "id": p.id,
            "created_at": p.created_at,
            "company": p.company_name,
            "total_weight_kg": float(p.total_weight_kg) if p.total_weight_kg else 0,
        }
        for p in _drafts
    ]

if not _draft_data:
    st.info("임시저장된 draft 발주가 없습니다. 위에서 먼저 임시저장을 해주세요.")
else:
    _selected_id = st.selectbox(
        "Draft 발주",
        options=[d["id"] for d in _draft_data],
        format_func=lambda i: (
            lambda d: f"#{d['id']} · {d['created_at'].strftime('%Y-%m-%d %H:%M')} · "
            f"{d['company']} · {d['total_weight_kg']:.1f}kg"
        )(next(d for d in _draft_data if d["id"] == i)),
        key="verify_draft_select",
    )

    with get_session() as _sess2:
        _plan = _sess2.get(InboundPlan, _selected_id)
        _items = (
            _sess2.execute(
                select(InboundPlanItem).where(
                    InboundPlanItem.plan_id == _selected_id,
                    InboundPlanItem.inbound_qty_final > 0,
                )
            )
            .scalars()
            .all()
        )
        _verify_company = _plan.company_name or "서현"
        _cp_masters = {m.coupang_option_id: m for m in _sess2.execute(
            select(CoupangProduct).where(CoupangProduct.company_name == _verify_company)
        ).scalars().all()}
        _wms_masters = {m.wms_barcode: m for m in _sess2.execute(
            select(WmsProduct).where(WmsProduct.company_name == _verify_company)
        ).scalars().all()}

    if not _items:
        st.warning("선택한 draft 에 확정 수량(>0) SKU가 없습니다.")
    else:
        # --- 쿠팡 결과 PDF 업로드 ---
        st.subheader("4-2. 쿠팡 결과 PDF 업로드")

        _uploaded = st.file_uploader(
            "쿠팡 결과 PDF 3개를 업로드",
            type=["pdf"],
            accept_multiple_files=True,
            key="verify_pdf_upload",
            help="① sku-barcode-labels-*.pdf\n② 물류부착문서_*.pdf\n③ 물류동봉문서_*.pdf",
        )

        _label_pdf = _attach_pdf = _invoice_pdf = None
        for f in _uploaded:
            if "label" in f.name.lower() or "barcode" in f.name.lower():
                _label_pdf = f
            elif "물류부착" in f.name or "부착문서" in f.name:
                _attach_pdf = f
            elif "물류동봉" in f.name or "동봉문서" in f.name:
                _invoice_pdf = f

        _mv_blob = _plan.movement_template_blob if _plan else None
        _mv_fname = _plan.movement_template_filename if _plan else None

        _vc = st.columns(4)
        with _vc[0]:
            st.write("✅ 라벨" if _label_pdf else "❌ 라벨")
        with _vc[1]:
            st.write("✅ 물류부착" if _attach_pdf else "❌ 물류부착")
        with _vc[2]:
            st.write("✅ 물류동봉" if _invoice_pdf else "⬜ 물류동봉")
        with _vc[3]:
            st.write("✅ 재고이동건(DB)" if _mv_blob else "⚠️ 없음")

        if _label_pdf and _attach_pdf:
            # --- 파싱 ---
            @st.cache_data(show_spinner="라벨 파싱...")
            def _vp_labels(data, name):
                return parse_barcode_labels(data)

            @st.cache_data(show_spinner="물류부착 파싱...")
            def _vp_attach(data, name):
                return parse_attachment_doc(data)

            _labels = _vp_labels(_label_pdf.getvalue(), _label_pdf.name)
            _attachment: AttachmentMeta = _vp_attach(_attach_pdf.getvalue(), _attach_pdf.name)

            _invoice: InvoiceMeta | None = None
            if _invoice_pdf:
                @st.cache_data(show_spinner="물류동봉 파싱...")
                def _vp_invoice(data, name):
                    return parse_invoice_doc(data)
                _invoice = _vp_invoice(_invoice_pdf.getvalue(), _invoice_pdf.name)

            # --- planned skus ---
            _planned: list[PlannedSku] = []
            for _it in _items:
                _cm = _cp_masters.get(_it.coupang_option_id)
                _own = _cm.wms_barcode if _cm else None
                _cbc = _cm.coupang_barcode if _cm else None
                _par = None
                _uq = 1
                if _own:
                    _wp = _wms_masters.get(_own)
                    if _wp:
                        _uq = int(_wp.unit_qty or 1)
                        _par = _wp.parent_wms_barcode if _wp.parent_wms_barcode and _wp.parent_wms_barcode != _own else None
                _sh = None
                if _own:
                    _wp2 = _wms_masters.get(_own)
                    _sh = _wp2.shelf_life_days if _wp2 else None
                if not _sh and _par:
                    _pwp = _wms_masters.get(_par)
                    _sh = _pwp.shelf_life_days if _pwp else None
                _emfg = None
                if _it.wms_short_expiry and _sh:
                    _emfg = _it.wms_short_expiry - timedelta(days=int(_sh) - 1)
                _planned.append(PlannedSku(
                    coupang_option_id=_it.coupang_option_id,
                    product_name=_it.product_name, option_name=_it.option_name,
                    inbound_qty=int(_it.inbound_qty_final or 0),
                    box_qty=int(_it.box_qty or 1), boxes=int(_it.inbound_boxes or 0),
                    own_wms_barcode=_own, parent_wms_barcode=_par, unit_qty=_uq,
                    coupang_barcode=_cbc,
                    sku_id=_cm.sku_id if _cm else None,
                    expects_label=False, expected_attached_barcode=None,
                    expected_expiry=_it.wms_short_expiry, expected_manufacture=_emfg,
                ))

            # 팔레트 배분
            _pa_items = [PA_PalletItem(key=s.coupang_option_id, name=s.product_name or "", boxes=s.boxes) for s in _planned if s.boxes > 0]
            _pa = pa_assign_pallets(_pa_items, pallet_size=cfg.pallet_size_boxes)

            # 중복 검사
            _dup = False
            _dup_info = None
            if _attachment.milkrun_id:
                with get_session() as _s3:
                    _ex = _s3.execute(
                        select(CoupangResultLog).where(
                            CoupangResultLog.company_name == cfg.default_company_name,
                            CoupangResultLog.milkrun_id == _attachment.milkrun_id,
                        )
                    ).scalar_one_or_none()
                    if _ex and _ex.plan_id != _selected_id:
                        _dup = True
                        _dup_info = f"밀크런ID {_attachment.milkrun_id} 이미 검수됨 (plan_id={_ex.plan_id})"

            # 재고이동건 합계
            _mvt_total = None
            if _mv_blob:
                _mvt_total = sum(s.inbound_qty for s in _planned if s.unit_qty and s.unit_qty >= 2 and s.inbound_qty > 0)

            # --- 검수 ---
            st.subheader("4-3. 검수 결과")
            _report = verify(
                planned_skus=_planned, labels=_labels, attachment=_attachment,
                pallet_assignment=_pa, duplicate_check=_dup, duplicate_info=_dup_info,
                movement_inbound_total=_mvt_total, invoice=_invoice,
            )
            if _report.overall == "ok":
                st.success("✅ 검수 통과")
            elif _report.overall == "warning":
                st.warning("⚠️ 일부 항목 확인 필요")
            else:
                st.error("❌ 검수 실패")

            _icon = {"ok": "✅", "warning": "⚠️", "fail": "❌"}
            for _ck in _report.checks:
                _lbl = f"{_icon.get(_ck.status, '•')} **{_ck.name}**"
                if _ck.expected is not None and _ck.actual is not None:
                    _lbl += f" — {_ck.actual} (예상 {_ck.expected})"
                elif _ck.actual is not None:
                    _lbl += f" — {_ck.actual}"
                st.markdown(_lbl)
                if _ck.detail:
                    st.caption(_ck.detail)
                if _ck.items:
                    with st.expander(f"세부 {len(_ck.items)}건"):
                        st.dataframe(pd.DataFrame(_ck.items), use_container_width=True, hide_index=True)

            # --- 2차 결과물 ---
            st.subheader("4-4. 2차 결과물 다운로드")
            _sec_items: list[SecondaryItem] = []
            for s in _planned:
                if s.boxes <= 0:
                    continue
                _cm2 = _cp_masters.get(s.coupang_option_id)
                _wp3 = _wms_masters.get(s.own_wms_barcode) if s.own_wms_barcode else None
                _pwp3 = _wms_masters.get(s.parent_wms_barcode) if s.parent_wms_barcode else None
                _wg = (_wp3.weight_g if _wp3 and _wp3.weight_g else 0) or (_pwp3.weight_g if _pwp3 and _pwp3.weight_g else 0)
                _shl = (_wp3.shelf_life_days if _wp3 else None) or (_pwp3.shelf_life_days if _pwp3 else None)
                _mfgd = None
                if s.expected_expiry and _shl:
                    _mfgd = s.expected_expiry - timedelta(days=int(_shl) - 1)
                _cpn = _cm2.product_name if _cm2 else (s.product_name or "")
                _cpo = _cm2.option_name if _cm2 else s.option_name
                # WMS 제품명 (취합리스트용)
                _wms_name = (_wp3.product_name if _wp3 and _wp3.product_name else None) or (_pwp3.product_name if _pwp3 and _pwp3.product_name else None)
                _sec_items.append(SecondaryItem(
                    coupang_option_id=s.coupang_option_id,
                    sku_id=_cm2.sku_id if _cm2 else None,
                    coupang_product_id=_cm2.coupang_product_id if _cm2 else None,
                    product_name=_cpn, option_name=_cpo,
                    wms_product_name=_wms_name,
                    own_wms_barcode=s.own_wms_barcode,
                    coupang_barcode=_cm2.coupang_barcode if _cm2 else None,
                    parent_wms_barcode=s.parent_wms_barcode,
                    unit_qty=s.unit_qty, inbound_qty=s.inbound_qty,
                    box_qty=s.box_qty, boxes=s.boxes,
                    weight_g=int(_wg or 0), expiry_date=s.expected_expiry,
                    manufacture_date=_mfgd, shelf_life_days=int(_shl) if _shl else None,
                ))

            _fc = _attachment.fc_name or "FC"
            _arr = _attachment.arrival_date or date.today()
            _yymmdd = _arr.strftime("%y%m%d")
            _datesuf = _arr.strftime("%Y%m%d")
            _yyyymm = _arr.strftime("%Y_%m월")

            # 요청ID = 발주서양식의 주문번호 base (취합리스트/팔레트적재리스트와 동일)
            _order_base = (_invoice.order_id if _invoice and _invoice.order_id else _attachment.milkrun_id) or ""
            _dc = st.columns(4)
            try:
                _cons = build_consolidation_list(_sec_items, _pa, _fc, _arr, cfg.default_company_name,
                    _invoice.order_id if _invoice and _invoice.order_id else _attachment.milkrun_id)
                with _dc[0]:
                    st.download_button("📥 취합리스트", data=_cons,
                        file_name=f"{cfg.default_company_name}_밀크런_취합리스트_{_yymmdd}_{_fc}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True, type="primary")
            except Exception as e:
                with _dc[0]:
                    st.error(str(e))
            try:
                _pal = build_pallet_loading_list(_sec_items, _pa, _fc, _arr,
                    milkrun_request_id=_invoice.order_id if _invoice and _invoice.order_id else _attachment.milkrun_id,
                    pallet_size=cfg.pallet_size_boxes)
                with _dc[1]:
                    st.download_button("📥 팔레트적재리스트", data=_pal,
                        file_name=f"밀크런_물류부착문서2 (팔레트적재리스트)_{_fc}_{_datesuf}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True, type="primary")
            except Exception as e:
                with _dc[1]:
                    st.error(str(e))
            if _mv_blob:
                try:
                    _mv_out = update_inventory_movement(bytes(_mv_blob), _sec_items, _arr, _fc, cfg.default_company_name)
                    with _dc[2]:
                        st.download_button("📥 재고이동건", data=_mv_out,
                            file_name=_mv_fname or f"쿠팡 재고이동건_{_yyyymm}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True, type="primary")
                except Exception as e:
                    with _dc[2]:
                        st.error(str(e))
            try:
                _ord = build_order_form(_sec_items, _fc, str(_order_base).strip(), pallet_assignment=_pa)
                with _dc[3]:
                    st.download_button("📥 발주서양식", data=_ord,
                        file_name=f"밀크런재고차감_로켓그로스({cfg.default_company_name}커머스)발주서양식_{_datesuf}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True, type="primary")
            except Exception as e:
                with _dc[3]:
                    st.error(str(e))

            # 쿠팡 PDF 리네임 다운로드
            st.markdown("##### 쿠팡 결과 PDF (파일명 변경)")
            _dpc = st.columns(3)
            if _invoice_pdf:
                with _dpc[0]:
                    st.download_button("📥 물류동봉문서(거래명세서)", data=_invoice_pdf.getvalue(),
                        file_name=f"밀크런_물류동봉문서(거래명세서)_{_fc}_{_datesuf}.pdf", mime="application/pdf", use_container_width=True)
            if _label_pdf:
                with _dpc[1]:
                    st.download_button("📥 제품 바코드라벨", data=_label_pdf.getvalue(),
                        file_name=f"제품 바코드라벨_{_fc}_{_datesuf}.pdf", mime="application/pdf", use_container_width=True)
            if _attach_pdf:
                with _dpc[2]:
                    st.download_button("📥 물류부착문서(팔레트부착)", data=_attach_pdf.getvalue(),
                        file_name=f"밀크런_물류부착문서1 (팔레트부착문서)_{_fc}_{_datesuf}.pdf", mime="application/pdf", use_container_width=True)

            # --- 발주 확정 ---
            st.divider()
            st.subheader("4-5. 발주 확정")
            if st.button("✅ 검수 완료 — 발주 확정", type="primary", use_container_width=True, disabled=_dup, key="verify_confirm"):
                try:
                    with get_session() as _s4:
                        _po = _s4.get(InboundPlan, _selected_id)
                        _po.status = "verified"
                        _po.fc_name = _fc
                        _po.arrival_date = _arr
                        _po.milkrun_id = _attachment.milkrun_id
                        _po.total_pallets = _pa.pallet_count
                        _po.verified_at = datetime.now(timezone.utc)
                        _po.confirmed_at = datetime.now(timezone.utc)
                        _ibo = {it.coupang_option_id: it for it in _items}
                        for _pi, _pal2 in enumerate(_pa.pallets, start=1):
                            for _en in _pal2:
                                _dbi = _ibo.get(_en.key)
                                if _dbi:
                                    _sk = next((s for s in _planned if s.coupang_option_id == _en.key), None)
                                    if _sk:
                                        _cm3 = _cp_masters.get(_sk.coupang_option_id)
                                        _bc = (_cm3.coupang_barcode if _cm3 and _cm3.coupang_barcode and _cm3.coupang_barcode.startswith("S0") else _sk.own_wms_barcode)
                                        _bt = "쿠팡바코드" if (_cm3 and _cm3.coupang_barcode and _cm3.coupang_barcode.startswith("S0")) else "88코드"
                                        _dbi.pallet_no = _pi
                                        _dbi.barcode_attached = _bc
                                        _dbi.barcode_type = _bt
                        _tb = sum(s.boxes for s in _planned)
                        _s4.add(CoupangResultLog(
                            company_name=cfg.default_company_name,
                            milkrun_id=_attachment.milkrun_id or "",
                            fc_name=_fc, arrival_date=_arr,
                            total_pallets=_pa.pallet_count, total_boxes=_tb,
                            total_skus=len([s for s in _planned if s.boxes > 0]),
                            plan_id=_selected_id,
                            label_filename=_label_pdf.name if _label_pdf else None,
                            attachment_filename=_attach_pdf.name if _attach_pdf else None,
                        ))
                        _s4.commit()
                    st.success(f"✅ 발주 #{_selected_id} 확정 완료")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"확정 실패: {e}")

            # --- 4-6. 재고차감 (3차 결과물) ---
            st.divider()
            st.subheader("4-6. 재고차감 (3차 결과물)")
            st.caption(
                "서현커머스에서 다운받은 **확장주문검색_*.xls** 파일을 업로드하면 발주서와 비교 검수 후 "
                "배송일괄처리양식 / 송장업로드양식 2개 파일을 생성합니다."
            )
            _os_file = st.file_uploader(
                "확장주문검색 파일 업로드",
                type=["xls", "xlsx"],
                key=f"order_search_{_selected_id}",
            )
            if _os_file is not None:
                try:
                    _os_rows = parse_order_search_file(_os_file.getvalue())
                except Exception as e:
                    st.error(f"파일 파싱 실패: {e}")
                    _os_rows = []

                if _os_rows:
                    st.info(f"파싱 완료: {len(_os_rows)}건")
                    _inv_qty_by_sku = None
                    if _invoice and _invoice.items:
                        _inv_qty_by_sku = {str(x.sku_id): int(x.confirmed_qty) for x in _invoice.items if x.sku_id}
                    _chk = validate_order_search(
                        _os_rows, _sec_items, str(_order_base).strip(),
                        pallet_assignment=_pa, invoice_qty_by_sku=_inv_qty_by_sku,
                    )
                    _basis_label = "쿠팡결과(거래명세서)" if _inv_qty_by_sku else "발주확정 계획"
                    if _chk.status == "ok":
                        st.success(f"✅ 검수 통과 — {_basis_label} 기준 일치")
                    else:
                        st.error(f"❌ 검수 실패 — {_basis_label} 기준 ({len(_chk.issues)}건 이슈)")
                        for _iss in _chk.issues:
                            st.markdown(f"- {_iss}")
                    if _chk.matched_pairs:
                        with st.expander(f"매칭 상세 {len(_chk.matched_pairs)}건"):
                            st.dataframe(pd.DataFrame(_chk.matched_pairs),
                                use_container_width=True, hide_index=True)

                    _dsuf = _arr.strftime("%Y%m%d")
                    _tc = st.columns(2)
                    try:
                        _bulk = build_shipping_bulk_form(_os_rows)
                        with _tc[0]:
                            st.download_button("📥 배송일괄처리양식", data=_bulk,
                                file_name=f"밀크런재고차감_배송일괄처리양식_{_dsuf}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True, type="primary",
                                disabled=(_chk.status != "ok"))
                    except Exception as e:
                        with _tc[0]:
                            st.error(str(e))
                    try:
                        _inv = build_invoice_upload_form(_os_rows)
                        with _tc[1]:
                            st.download_button("📥 송장업로드양식", data=_inv,
                                file_name=f"밀크런재고차감_송장업로드양식_{_dsuf}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                use_container_width=True, type="primary",
                                disabled=(_chk.status != "ok"))
                    except Exception as e:
                        with _tc[1]:
                            st.error(str(e))

                    # --- 재고차감 완료 ---
                    if st.button(
                        "✅ 재고차감 완료",
                        type="primary", use_container_width=True,
                        disabled=(_chk.status != "ok"),
                        key=f"stock_deduction_done_{_selected_id}",
                    ):
                        try:
                            with get_session() as _s5:
                                _po2 = _s5.get(InboundPlan, _selected_id)
                                _po2.status = "completed"
                                _s5.commit()
                            st.success(f"✅ 발주 #{_selected_id} 재고차감 완료 — 상태: completed")
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"상태 업데이트 실패: {e}")
        else:
            st.info("라벨 PDF와 물류부착문서 PDF를 업로드하세요.")
