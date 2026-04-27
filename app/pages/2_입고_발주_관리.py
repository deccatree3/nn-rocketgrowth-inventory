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
    extract_template_option_ids,
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
    PlanFile,
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
    raw_files: dict[str, tuple[str, bytes]] | None = None,
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
        # raw_files 저장 (PlanFile)
        if raw_files:
            for ftype, (fname, fbytes) in raw_files.items():
                session.merge(PlanFile(
                    plan_id=plan.id, file_type=ftype,
                    file_name=fname, content=fbytes,
                ))
        session.commit()
        return plan.id


def _save_plan_files(plan_id: int, files: dict[str, tuple[str, bytes]]):
    """기존 plan 에 파일을 추가/갱신."""
    with get_session() as session:
        for ftype, (fname, fbytes) in files.items():
            existing = session.execute(
                select(PlanFile).where(
                    PlanFile.plan_id == plan_id, PlanFile.file_type == ftype
                )
            ).scalar_one_or_none()
            if existing:
                existing.file_name = fname
                existing.content = fbytes
            else:
                session.add(PlanFile(
                    plan_id=plan_id, file_type=ftype,
                    file_name=fname, content=fbytes,
                ))
        session.commit()


def _load_plan_files(plan_id: int) -> dict[str, tuple[str, bytes]]:
    """plan_id 에 연결된 파일 로드. {file_type: (file_name, content)}"""
    with get_session() as session:
        rows = session.execute(
            select(PlanFile).where(PlanFile.plan_id == plan_id)
        ).scalars().all()
        return {r.file_type: (r.file_name, bytes(r.content)) for r in rows}


STATUS_LABELS = {"draft": "📝 임시저장", "verified": "✅ 발주확정", "completed": "🏁 완료"}


# ---------------------------------------------------------------------------
# 단계별 UI helper (5단계 위저드)
# ---------------------------------------------------------------------------
_WIZARD_STEPS = [
    ("①", "", "기초자료 업로드"),
    ("②", "", "발주 수량 확정"),
    ("③", "", "쿠팡 입고생성 파일 생성"),
    ("④", "", "쿠팡 입고생성 결과물 검수"),
    ("⑤", "", "물류센터 전달 파일 생성"),
    ("⑥", "", "이지어드민 재고 차감"),
]


def _render_stepper(current: int, completed: set[int] | frozenset[int] = frozenset()) -> str:
    """5단계 가로 스테퍼 HTML."""
    cells: list[str] = []
    for idx, (no, icon, label) in enumerate(_WIZARD_STEPS, start=1):
        if idx in completed:
            bg, color, weight, mark = "#e8f7ee", "#0a7", "600", "✅"
        elif idx == current:
            bg, color, weight, mark = "#3b82f6", "#fff", "700", no
        else:
            bg, color, weight, mark = "#f3f4f6", "#888", "400", no
        cells.append(
            f'<div style="flex:1; min-width:0; padding:10px 8px; '
            f'background:{bg}; color:{color}; border-radius:6px; '
            f'font-weight:{weight}; text-align:center; '
            f'white-space:nowrap; overflow:hidden; text-overflow:ellipsis; '
            f'font-size:0.9em;">'
            f'<span style="font-size:1.05em; margin-right:4px;">{mark}</span>'
            f'<span>{(icon + " ") if icon else ""}{label}</span>'
            f'</div>'
        )
        if idx < len(_WIZARD_STEPS):
            cells.append(
                '<div style="flex:0 0 16px; text-align:center; color:#bbb;">→</div>'
            )
    return (
        '<div style="display:flex; align-items:center; gap:0; '
        'margin:6px 0 14px 0;">' + "".join(cells) + "</div>"
    )


def _render_context_bar(plan, has_pdfs: bool = False) -> str:
    """관리 모드 상단 회차 컨텍스트 바."""
    sid = f"#{plan.id}"
    status_label = STATUS_LABELS.get(plan.status or "draft", plan.status or "?")
    company = plan.company_name or "—"
    fc = plan.fc_name or "미정"
    arr = plan.arrival_date or plan.plan_date or "미정"
    worker = plan.worker or "미정"
    milkrun = plan.milkrun_id or "미정"
    parts = [
        f'<span style="background:#fef3c7; color:#92400e; padding:3px 8px; '
        f'border-radius:4px; font-weight:700;">{sid}</span>',
        f'<span>{status_label}</span>',
        f'<span><b>업체</b> {company}</span>',
        f'<span><b>FC</b> {fc}</span>',
        f'<span><b>입고일</b> {arr}</span>',
        f'<span><b>작업자</b> {worker}</span>',
        f'<span><b>milkrun_id</b> {milkrun}</span>',
    ]
    return (
        '<div style="display:flex; flex-wrap:wrap; gap:12px; align-items:center; '
        'padding:8px 12px; background:#f9fafb; border:1px solid #e5e7eb; '
        'border-radius:6px; margin:0 0 10px 0; font-size:0.92em;">'
        + "".join(parts) + "</div>"
    )


def _management_current_step(status: str, has_pdfs: bool) -> int:
    """관리 모드 현재 단계 추정."""
    if status == "completed":
        return 6
    if status == "verified":
        return 5  # 발주 확정 완료, ⑤·⑥(물류센터/이지어드민) 진행 중
    # status == "draft"
    if has_pdfs:
        return 4  # 검수 진행 중
    return 3


# ---------------------------------------------------------------------------
# 페이지
# ---------------------------------------------------------------------------
st.set_page_config(page_title="입고 발주 관리", page_icon="📦", layout="wide")
st.title("📦 입고 발주 관리")

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

# ============================================================================
# 모드 선택: 신규 계획 vs 기존 계획 관리
# ============================================================================
with get_session() as _all_sess:
    _all_plans = _all_sess.execute(
        select(InboundPlan).order_by(desc(InboundPlan.created_at))
    ).scalars().all()

_plan_options = ["➕ 신규 계획"] + [
    f"#{p.id} {STATUS_LABELS.get(p.status, p.status)} · {p.company_name} · "
    f"{p.arrival_date or p.plan_date or ''}"
    + (f" · {p.fc_name}" if p.fc_name else "")
    for p in _all_plans
]

# 저장 직후 자동 전환: 위젯 생성 전에 session_state 설정
_pending_id = st.session_state.pop("_pending_plan_id", None)
if _pending_id is not None:
    _matched = next((opt for opt in _plan_options if opt.startswith(f"#{_pending_id} ")), None)
    if _matched:
        st.session_state["plan_mode_select"] = _matched

_selected_mode = st.selectbox("발주 계획", _plan_options, key="plan_mode_select")
_is_new = _selected_mode.startswith("➕")
_selected_plan_id = int(_selected_mode.split("#")[1].split(" ")[0]) if not _is_new else None

if _is_new:
        # ====================================================================
        # 신규 계획 모드 (단계 ① → ②)
        # ====================================================================

    # 상단 5단계 스테퍼 (현재 단계는 파일 업로드 여부로 판정 — 파일 분류 후 다시 갱신)
    _stepper_ph = st.empty()
    _stepper_ph.markdown(_render_stepper(current=1, completed=set()), unsafe_allow_html=True)

        # --- ① 기초자료 업로드 -----------------------------------------------------
    st.subheader("① 기초자료 업로드")

    from lib.file_classifier import (
        FILE_TYPE_COUPANG, FILE_TYPE_WMS, FILE_TYPE_TEMPLATE, FILE_TYPE_MOVEMENT,
        FILE_TYPE_LABELS, CompanyFileGroup, classify_uploaded_files,
    )

    st.caption("기초자료 4개 파일을 업로드해주세요.")

    _UPLOAD_GUIDE_ROWS = [
        ("WMS 재고 파일", FILE_TYPE_WMS,
         "다원WMS > 재고관리 > 창고별로케이션별재고(OWNER) > [품목-정상,불량-로케이션-로트] 탭 > 검색 > 우클릭, Export(Excel)",
         "Document_YYYY-MM-DD.xls"),
        ("쿠팡 재고 파일", FILE_TYPE_COUPANG,
         "쿠팡Wing > 로켓그로스 > 재고현황 > 엑셀 다운로드",
         "inventory_health_sku_info_YYYYMMDDhhmmss.xlsx"),
        ("쿠팡 입고생성 파일", FILE_TYPE_TEMPLATE,
         "쿠팡Wing > 로켓그로스 > 입고관리 > 새로운 입고 생성 > 엑셀 다운로드",
         "generated_excel.xlsx"),
        ("재고이동 파일", FILE_TYPE_MOVEMENT,
         "이번달 '쿠팡 재고이동건' 파일",
         "쿠팡 재고이동건_YYYY_MM월.xlsx"),
    ]
    _GUIDE_COMPANIES = ["캐처스", "서현"]
    # 업체별로 해당 파일 타입이 불필요(음영 처리)한 조합
    _GUIDE_NA = {("캐처스", FILE_TYPE_MOVEMENT)}

    def _render_upload_guide(groups: dict | None):
        comp_header = "".join(
            f'<th style="width:90px; text-align:center; white-space:nowrap;">{c}</th>' for c in _GUIDE_COMPANIES
        )
        body = ""
        for label, ft, path, fname_example in _UPLOAD_GUIDE_ROWS:
            marks = ""
            for c in _GUIDE_COMPANIES:
                if (c, ft) in _GUIDE_NA:
                    marks += (
                        '<td style="width:90px; text-align:center; white-space:nowrap; '
                        'background-color:#eee; color:#888;">—</td>'
                    )
                    continue
                g = groups.get(c) if groups else None
                mark = "✅" if g and ft in g.files else ""
                marks += f'<td style="width:90px; text-align:center; white-space:nowrap;">{mark}</td>'
            body += (
                f"<tr><td>{label}</td>"
                f'<td><code style="font-size:0.85em;">{fname_example}</code></td>'
                f"<td>{path}</td>{marks}</tr>"
            )
        return (
            '<table style="border-collapse: collapse; width: 100%;">'
            '<thead><tr>'
            '<th style="text-align:left;">구분</th>'
            '<th style="text-align:left;">파일명 예시</th>'
            '<th style="text-align:left;">취합 경로</th>'
            f'{comp_header}'
            '</tr></thead>'
            f'<tbody>{body}</tbody>'
            '</table>'
        )

    _guide_ph = st.empty()
    _guide_ph.markdown(_render_upload_guide(None), unsafe_allow_html=True)

    uploaded_files = st.file_uploader(
        "파일 업로드",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        key="multi_upload",
        label_visibility="collapsed",
    )

    if not uploaded_files:
        st.info("파일을 업로드하세요. 업체별 파일 4종을 한 번에 올릴 수 있습니다.")
        st.stop()

    # 자동 분류
    classified, company_groups = classify_uploaded_files(uploaded_files)
    _guide_ph.markdown(_render_upload_guide(company_groups), unsafe_allow_html=True)

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

    # 업체별 선택사항 파일 (예: 캐처스는 재고이동건 불필요)
    _optional_by_company = {"캐처스": {FILE_TYPE_MOVEMENT}}
    _optional_for_this = _optional_by_company.get(selected_company, set())

    _missing_required = [ft for ft in grp.missing_types if ft not in _optional_for_this]
    if _missing_required:
        missing_labels = [FILE_TYPE_LABELS[ft] for ft in _missing_required]
        st.info(f"**{selected_company}** 미감지 파일: {', '.join(missing_labels)}")

    _required_ok = coupang_file and wms_file and template_file and (
        movement_file or FILE_TYPE_MOVEMENT in _optional_for_this
    )
    if not _required_ok:
        _need = 3 if FILE_TYPE_MOVEMENT in _optional_for_this else 4
        st.warning(f"**{selected_company}** 의 필수 파일 {_need}종이 모두 필요합니다.")
        st.stop()
    # (스테퍼는 '제출' 버튼 클릭 후 ②로 전환 — 단순 파일 도착만으로는 step 1 유지)


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

    # 쿠팡 업로드양식(generated_excel)에 존재하는 옵션 ID 로 필터링
    # (캐시 미사용 — 이전 버그 캐시 회피)
    _tpl_tmp = Path("./_tmp_tpl_" + template_file.name)
    _tpl_tmp.write_bytes(template_file.getvalue())
    try:
        tpl_option_ids = extract_template_option_ids(_tpl_tmp)
    finally:
        try:
            _tpl_tmp.unlink()
        except Exception:
            pass
    _cp_total_before = len(cp_snap.rows)
    if tpl_option_ids:
        cp_snap.rows = [r for r in cp_snap.rows if r.coupang_option_id in tpl_option_ids]

    st.success(
        f"파싱 완료: 쿠팡 {len(cp_snap.rows)}/{_cp_total_before}건 "
        f"(업로드양식 {len(tpl_option_ids)}개 옵션 기준, {cp_snap.snapshot_date}) / "
        f"WMS {len(wms_snap.rows)}행 → {len(wms_agg)} 바코드 ({wms_snap.snapshot_date}) "
        f"— RELEASEAREA 제외"
    )

    # === ① 끝: '제출' 버튼으로 ②로 진행 (파일이 바뀌면 재제출 필요) ===
    import hashlib as _hashlib
    _files_fp = _hashlib.md5(
        "|".join(sorted(f"{f.name}-{getattr(f, 'size', 0)}" for f in uploaded_files)).encode()
    ).hexdigest()
    if st.session_state.get("_step1_files_fp") != _files_fp:
        st.session_state["_step1_submitted"] = False
        st.session_state["_step1_files_fp"] = _files_fp

    if not st.session_state.get("_step1_submitted"):
        if st.button("제출", type="primary", use_container_width=True, key="_step1_submit_btn"):
            st.session_state["_step1_submitted"] = True
            st.rerun()
        st.stop()

    # ① 단계 완료 → 스테퍼를 ②로 진행 표시
    _stepper_ph.markdown(
        _render_stepper(current=2, completed={1}),
        unsafe_allow_html=True,
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

    # --- 4-2. 팔레트 최적화 (토글 위젯은 ② 섹션 내부에서 렌더, 여기서는 값만 읽음) ---
    pallet_on = st.session_state.get("_pallet_on_widget", True)

    # 팔레트 토글이 바뀌면 편집 세션 잔재 제거 — 바뀐 추천값이 그대로 반영되도록
    _prev_pallet_on = st.session_state.get("_pallet_on_prev")
    if _prev_pallet_on is not None and _prev_pallet_on != pallet_on:
        for _k in list(st.session_state.keys()):
            if isinstance(_k, str) and _k.startswith("inbound_final_by_opt::"):
                st.session_state.pop(_k, None)
        st.session_state.pop("inbound_final_by_opt", None)
    st.session_state["_pallet_on_prev"] = pallet_on

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
            pallet_size=cfg.pallet_size_boxes,
            overstock_days=None,    # 과잉재고 상한 제거 — 무조건 팔레트 꽉 채움
            rounding="up",
            cap_per_sku=None,       # 쏠림 상한 제거
        )
        # 결과 주입 — 팔레트 최적화가 켜져있으면 confirmed(=inbound_final) 기본값도 팔레트 값으로
        for i, row in base_df.iterrows():
            key = int(row["coupang_option_id"])
            opt_boxes = int(pallet_result.optimized_boxes.get(key, row["basic_boxes"] or 0))
            opt_qty = opt_boxes * int(row["box_qty"] or 1)
            base_df.at[i, "pallet_boxes"] = opt_boxes
            base_df.at[i, "inbound_pallet"] = opt_qty
            base_df.at[i, "pallet_adjusted"] = opt_boxes != int(row["basic_boxes"] or 0)
            base_df.at[i, "inbound_final"] = opt_qty  # 확정수량 기본값 = 팔레트 최적화 결과
    else:
        pallet_result = None


    # --- 5. 세션 상태 & 편집 UI -----------------------------------------------
    editor_key = f"editor_{cp_snap.snapshot_date}_{wms_snap.snapshot_date}"

    # 확정수량 편집 session 은 스냅샷 단위로 격리 (다른 스냅샷 잔재 차단)
    _session_key = f"inbound_final_by_opt::{cp_snap.snapshot_date}::{wms_snap.snapshot_date}"
    if _session_key not in st.session_state:
        st.session_state[_session_key] = {}
    # 레거시 글로벌 키 cleanup (배포 전 잔재)
    st.session_state.pop("inbound_final_by_opt", None)

    # base_df 에 세션 값 주입 (사용자가 실제 입력한 값이 있을 때만)
    for i, row in base_df.iterrows():
        opt = int(row["coupang_option_id"])
        if opt in st.session_state[_session_key]:
            base_df.at[i, "inbound_final"] = st.session_state[_session_key][opt]


    # --- 6. 부모 풀 할당 수행 --------------------------------------------------
    def _allocate(df: pd.DataFrame) -> pd.DataFrame:
        """부모 WMS바코드 그룹별로 순차 할당하고 결과 컬럼을 부착."""
        df = df.copy()
        df["selected_batch_expiry"] = None
        df["selected_status"] = None
        df["pool_total_base"] = None
        df["pool_remaining_base"] = None
        df["max_single_batch_after"] = None

        # WMS agg 을 대소문자·공백 정규화된 키로 보조 인덱스화 (fallback lookup)
        _wms_agg_norm = {str(k).strip().upper(): v for k, v in wms_agg.items()}

        for parent_bc, group in df.groupby("parent_wms_barcode", sort=False, dropna=False):
            if not parent_bc:
                # 부모 정보 없음 → 상태만 마킹
                for idx in group.index:
                    df.at[idx, "selected_status"] = "no_parent"
                continue
            agg = wms_agg.get(parent_bc) or _wms_agg_norm.get(str(parent_bc).strip().upper())
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

    # --- 6a. WMS 매칭 진단 (현재고 0 원인 확인용) -----------------------------
    _wms_keys_norm = {str(k).strip().upper() for k in wms_agg.keys()}
    _missing_parents = []
    for _bc in base_df["parent_wms_barcode"].dropna().unique():
        if str(_bc).strip().upper() not in _wms_keys_norm:
            _rows = base_df[base_df["parent_wms_barcode"] == _bc]
            _names = _rows["product_name"].dropna().unique().tolist()
            _missing_parents.append({
                "parent_wms_barcode": _bc,
                "상품명": ", ".join(_names[:2]),
                "SKU수": len(_rows),
            })
    if _missing_parents:
        with st.expander(
            f"⚠️ WMS 파일에서 못 찾은 parent 바코드 ({len(_missing_parents)}건) — 해당 제품은 현재고 0 으로 표시됨",
            expanded=False,
        ):
            st.caption(
                "원인 후보: (1) WMS 파일에 해당 바코드 재고 없음 "
                "(2) 제품 마스터의 parent_wms_barcode 가 실제 WMS 바코드와 다름 "
                "(3) 모든 재고가 RELEASEAREA(출고대기) LOC 에 있음."
            )
            st.dataframe(pd.DataFrame(_missing_parents), use_container_width=True, hide_index=True)

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
    st.subheader("② 발주 수량 확정")

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

    # 풀 내 '단품 SKU' 상품명을 대표로 선택 (단품이 실제 생산·재발주 대상)
    single_product_per_pool = (
        allocated_df[
            (allocated_df["parent_wms_barcode"].notna())
            & (allocated_df["unit_qty"].fillna(1).astype(int) == 1)
        ]
        .groupby("parent_wms_barcode", sort=False)["product_name"]
        .first()
    )
    pool_stats["single_product"] = (
        pool_stats["parent_wms_barcode"].map(single_product_per_pool).fillna(pool_stats["first_product"])
    )

    repro_list = pool_stats[pool_stats["needs_reproduction"]].sort_values("shortfall", ascending=False)

    with st.expander(
        "🏭 재발주 필요 품목 Check"
        + (f" · 재생산 리드타임 {reproduction_lead}일 기준 산계" if len(repro_list) > 0 else ""),
        expanded=len(repro_list) > 0,
    ):
        if len(repro_list) == 0:
            st.caption("✅ 단품 재고가 재생산 리드타임 동안 자력 운영 가능")
        else:
            display = repro_list[
                [
                    "parent_wms_barcode",
                    "single_product",
                    "pool_total",
                    "allocated_base",
                    "pool_remaining",
                    "pool_velocity",
                    "reproduction_demand",
                    "shortfall",
                ]
            ].copy()
            # 정수 변환 (소수점 제거) + 28일후부족은 음수로 표기 (부족량 강조)
            for _c in ["pool_velocity", "reproduction_demand", "shortfall"]:
                display[_c] = pd.to_numeric(display[_c], errors="coerce").fillna(0)
            display["pool_velocity"] = display["pool_velocity"].round(0).astype(int)
            display["reproduction_demand"] = display["reproduction_demand"].round(0).astype(int)
            display["shortfall"] = (-display["shortfall"]).round(0).astype(int)
            display = display.rename(
                columns={
                    "parent_wms_barcode": "WMS바코드",
                    "single_product": "상품명",
                    "pool_total": "현재고",
                    "allocated_base": "이번출고",
                    "pool_remaining": "출고후잔여",
                    "pool_velocity": "일소요",
                    "reproduction_demand": f"{reproduction_lead}일소요",
                    "shortfall": f"{reproduction_lead}일후부족",
                }
            )
            styled = display.style.map(
                lambda v: "color: red; font-weight: bold;" if isinstance(v, (int, float)) and v < 0 else "",
                subset=[f"{reproduction_lead}일후부족"],
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)
            st.warning(
                f"⚠️ {len(repro_list)}개 품목이 재생산 리드타임({reproduction_lead}일) 동안 버티지 못합니다. "
                "생산/발주 담당자에게 재발주 검토를 요청해주세요."
            )

    # --- 7-2. 팔레트 단위 최적화 토글 (재발주 Check 아래, 검색 위) ---------------
    st.checkbox(
        f"🚛 팔레트 단위 최적화 (1팔레트 = {cfg.pallet_size_boxes}박스)",
        value=pallet_on,
        key="_pallet_on_widget",
        help=(
            f"체크 시: 총 박스수가 {cfg.pallet_size_boxes}의 배수가 되도록 항상 올림하여 팔레트를 꽉 채움. "
            "체크 해제 시: 엔진 기본 추천값 그대로 사용 (팔레트 미충족 가능)."
        ),
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
        search = st.text_input(
            "🔍 상품명 / 옵션ID 검색",
            help="여러 개를 쉼표(,) 또는 공백으로 구분해 동시에 적용. 예: '비타민, 94917143993'",
        )
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
        import re as _re
        terms = [t.strip() for t in _re.split(r"[,\s]+", search) if t.strip()]
        if terms:
            name_series = view["product_name"].fillna("").astype(str)
            opt_series = view["coupang_option_id"].fillna("").astype(str)
            mask = pd.Series(False, index=view.index)
            for t in terms:
                tl = t.lower()
                mask = mask | name_series.str.lower().str.contains(tl, regex=False) | opt_series.str.contains(t, regex=False)
            view = view[mask]
    if status_filter:
        view = view[view["urgency"].isin(status_filter)]

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
    # 팔레트 꽉 채움으로 증가한 SKU 강조 (옅은 청록)
    PALLET_ADJUSTED_BG = "background-color: #cceeff; font-weight: bold;"


    def _highlight_over(row):
        """재고 over → 빨강, 팔레트조정 → 청록, 정상 → 기본 노랑."""
        styles = [""] * len(row)
        pool_rem = row.get("pool_remaining_base")
        status = row.get("selected_status")
        is_over = (
            (pool_rem is not None and not (isinstance(pool_rem, float) and pd.isna(pool_rem)) and pool_rem < 0)
            or status == "insufficient"
        )
        cols = list(row.index)
        if is_over:
            if "inbound_final" in cols:
                styles[cols.index("inbound_final")] = OVER_CONFIRM_BG
            for col in ("pool_remaining_base", "pool_remaining_bundle"):
                if col in cols:
                    styles[cols.index(col)] = OVER_STOCK_BG
        else:
            # 팔레트 꽉 채움으로 증가한 경우 구분 표시
            try:
                inbound_final = row.get("inbound_final")
                basic_boxes = row.get("basic_boxes")
                box_qty = row.get("box_qty")
                if (
                    inbound_final is not None
                    and not (isinstance(inbound_final, float) and pd.isna(inbound_final))
                    and basic_boxes is not None
                    and not (isinstance(basic_boxes, float) and pd.isna(basic_boxes))
                    and box_qty
                    and int(inbound_final) != int(basic_boxes) * int(box_qty)
                ):
                    if "inbound_final" in cols:
                        styles[cols.index("inbound_final")] = PALLET_ADJUSTED_BG
                    if "confirmed_boxes" in cols:
                        styles[cols.index("confirmed_boxes")] = PALLET_ADJUSTED_BG
            except (ValueError, TypeError):
                pass
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
            "coupang_option_id": st.column_config.NumberColumn(
                "옵션ID",
                format="%d",
                width="small",
                pinned=True,
                help="쿠팡 옵션 ID",
            ),
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
                "입고권장(낱개)",
                format="%d",
                help="엔진 기본 추천 낱개 수량 — 팔레트 꽉 채움 적용 전",
            ),
            "basic_boxes": st.column_config.NumberColumn(
                "입고권장(box)",
                format="%d",
                help="엔진 기본 추천 박스수 — 팔레트 꽉 채움 적용 전 (입고권장(낱개) ÷ box입인)",
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
            if opt in st.session_state[_session_key]:
                del st.session_state[_session_key][opt]
                changed = True
        else:
            new_val = _ni(raw_val) or 0
            if st.session_state[_session_key].get(opt) != new_val:
                st.session_state[_session_key][opt] = new_val
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
    # 확정 수량 기반 — allocated_df(전체) + edited(필터 영역의 사용자 편집) 병합
    # 기존에 edited 만 썼더니 상태 필터로 숨겨진 SKU 가 누락되어 총합이 과소 계산됐음
    _edited_qty_by_opt = {
        int(r["coupang_option_id"]): (None if pd.isna(r.get("inbound_final")) else int(r["inbound_final"]))
        for _, r in edited.iterrows()
    }
    confirmed_qty = 0
    confirmed_boxes_sum = 0
    active_cnt = 0
    total_weight_g = 0  # 총중량 (g)
    for _, r in allocated_df.iterrows():
        opt_id = int(r["coupang_option_id"])
        if opt_id in _edited_qty_by_opt:
            qty = _edited_qty_by_opt[opt_id] or 0
        else:
            raw = r.get("inbound_final")
            qty = int(raw) if raw is not None and not (isinstance(raw, float) and pd.isna(raw)) else 0
        if qty > 0:
            active_cnt += 1
            box = int(r.get("box_qty") or 1)
            boxes = qty // max(box, 1)
            confirmed_qty += qty
            confirmed_boxes_sum += boxes
            unit_w = int(r.get("weight_g") or 0)
            total_weight_g += unit_w * qty + 500 * boxes

    total_weight_kg = total_weight_g / 1000

    _pallet_sz = cfg.pallet_size_boxes
    confirmed_pallets_float = confirmed_boxes_sum / _pallet_sz if confirmed_boxes_sum else 0

    # 기본/팔레트 추천 합계 (비교용)
    basic_boxes_sum = int(allocated_df["basic_boxes"].fillna(0).sum())
    pallet_boxes_sum = int(allocated_df["pallet_boxes"].fillna(0).sum())

    col_s1, col_s2, col_s3, col_s4, col_s5 = st.columns(5)
    with col_s1:
        st.metric("확정 수량 (낱개)", f"{confirmed_qty:,}")
    with col_s2:
        st.metric("확정 박스수", f"{confirmed_boxes_sum:,}")
    with col_s3:
        pallet_full = confirmed_boxes_sum // _pallet_sz
        pallet_remainder = confirmed_boxes_sum % _pallet_sz
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

    # 팔레트 최적화 상세 (사용자 확정 박스수가 이미 꽉찬 팔레트면 표시 생략)
    _pallets_already_full = confirmed_boxes_sum > 0 and confirmed_boxes_sum % _pallet_sz == 0
    if pallet_on and pallet_result is not None and pallet_result.mode != "noop" and not _pallets_already_full:
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

    # === ② 끝: 발주 수량 확정 버튼 ===
    st.divider()
    if confirmed_qty == 0:
        st.button(
            "발주 수량 확정",
            disabled=True,
            use_container_width=True,
            help="확정 수량을 1개 이상 입력해야 다음 단계로 진행할 수 있습니다.",
        )
        st.caption("확정 수량을 입력한 후 이 버튼을 누르면 발주가 저장되고 ③ 단계로 진행합니다.")
    else:
        if st.button(
            "발주 수량 확정",
            type="primary",
            use_container_width=True,
            help="현재 입력한 확정 수량을 저장하고 ③ 쿠팡 업로드 파일 단계로 이동합니다.",
        ):
            try:
                save_df = allocated_df.copy()
                for _, erow in edited.iterrows():
                    opt = int(erow["coupang_option_id"])
                    mask = save_df["coupang_option_id"] == opt
                    save_df.loc[mask, "inbound_final"] = _ni(erow["inbound_final"]) or 0
                _raw_files: dict[str, tuple[str, bytes]] = {}
                if coupang_file:
                    _raw_files["coupang_raw"] = (coupang_file.name, coupang_file.getvalue())
                if wms_file:
                    _raw_files["wms_raw"] = (wms_file.name, wms_file.getvalue())
                if template_file:
                    _raw_files["template"] = (template_file.name, template_file.getvalue())
                plan_id = _save_plan(
                    cp_snap=cp_snap,
                    wms_snap=wms_snap,
                    full_df=save_df,
                    company_name=cfg.default_company_name,
                    shipment_type=cfg.default_shipment_type,
                    total_weight_kg=total_weight_kg,
                    movement_blob=movement_file.getvalue() if movement_file else None,
                    movement_filename=movement_file.name if movement_file else None,
                    raw_files=_raw_files,
                )
                st.success(
                    f"발주 수량 확정 완료 (plan_id={plan_id}). ③ 쿠팡 입고생성 업로드 파일 단계로 이동합니다."
                )
                st.session_state["_pending_plan_id"] = plan_id
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")



else:
    # ====================================================================
    # 기존 계획 관리 모드
    # ====================================================================
    _mgmt_plan = next((p for p in _all_plans if p.id == _selected_plan_id), None)
    if not _mgmt_plan:
        st.error("선택한 계획을 찾을 수 없습니다.")
        st.stop()

    _mgmt_status = _mgmt_plan.status or "draft"
    _mgmt_company = _mgmt_plan.company_name or "서현"
    _is_completed = _mgmt_status == "completed"

    # === 상단: 회차 컨텍스트 바 + 5단계 스테퍼 ===
    st.markdown(_render_context_bar(_mgmt_plan), unsafe_allow_html=True)

    # 스테퍼: status 기반 단계 추정. 1·2 단계는 항상 완료 (저장된 계획).
    _mgmt_step = _management_current_step(_mgmt_status, has_pdfs=False)
    _mgmt_completed: set[int] = {1, 2}
    if _mgmt_step >= 4:
        _mgmt_completed.add(3)
    if _mgmt_status in ("verified", "completed"):
        _mgmt_completed.add(4)
    if _mgmt_status == "completed":
        _mgmt_completed.add(5)
        _mgmt_completed.add(6)
    st.markdown(
        _render_stepper(current=_mgmt_step, completed=_mgmt_completed),
        unsafe_allow_html=True,
    )

    # --- 공통 데이터 로드 ---
    with get_session() as _ms:
        _mgmt_items = _ms.execute(
            select(InboundPlanItem).where(
                InboundPlanItem.plan_id == _selected_plan_id,
                InboundPlanItem.inbound_qty_final > 0,
            )
        ).scalars().all()
        _mgmt_cp = {m.coupang_option_id: m for m in _ms.execute(
            select(CoupangProduct).where(CoupangProduct.company_name == _mgmt_company)
        ).scalars().all()}
        _mgmt_wms = {m.wms_barcode: m for m in _ms.execute(
            select(WmsProduct).where(WmsProduct.company_name == _mgmt_company)
        ).scalars().all()}

    _mgmt_files = _load_plan_files(_selected_plan_id)

    if not _mgmt_items:
        st.warning("이 계획에 확정 수량(>0) SKU가 없습니다.")
        st.stop()

    # === ③ 쿠팡 입고생성 업로드 파일 (계획 요약 포함) ===
    _step3_label = "③ 쿠팡 입고생성 파일 생성"
    if _mgmt_step >= 4:
        _step3_label += " ✅"
    st.subheader(_step3_label)
    st.caption("아래의 버튼을 클릭해서 파일을 다운로드 후 쿠팡의 입고관리에 업로드 해주세요.")
    _plan_df = pd.DataFrame([
        {
            "상품명": i.product_name,
            "7일판매": i.sales_7d,
            "30일판매": i.sales_30d,
            "현재재고": i.current_total_stock,
            "박스낱수": i.box_qty,
            "추천입고": i.inbound_qty_suggested,
            "확정입고": i.inbound_qty_final,
            "확정박스": i.inbound_boxes,
            "팔레트": i.pallet_no,
        }
        for i in _mgmt_items
    ])
    _total_qty = int(_plan_df["확정입고"].sum())
    _total_boxes = int(_plan_df["확정박스"].sum())
    # 팔레트 수: 저장값 우선, 없으면 박스수로 계산 (올림)
    _psz = cfg.pallet_size_boxes
    _pallet_cnt = _mgmt_plan.total_pallets or ((_total_boxes + _psz - 1) // _psz if _total_boxes else 0)
    _pallet_disp = f"{_pallet_cnt}" + (" (꽉참)" if _total_boxes and _total_boxes % _psz == 0 else "")
    _weight_kg = float(_mgmt_plan.total_weight_kg) if _mgmt_plan.total_weight_kg else 0.0
    _mc1, _mc2, _mc3, _mc4, _mc5 = st.columns(5)
    with _mc1:
        st.metric("SKU", f"{len(_mgmt_items)}")
    with _mc2:
        st.metric("확정수량", f"{_total_qty:,}")
    with _mc3:
        st.metric("박스수", f"{_total_boxes:,}")
    with _mc4:
        st.metric("팔레트", _pallet_disp)
    with _mc5:
        st.metric("총중량 (kg)", f"{_weight_kg:,.1f}")
    st.dataframe(_plan_df, use_container_width=True, hide_index=True, height=300)

    # 쿠팡 업로드 양식 재생성
    if "template" in _mgmt_files and not _is_completed:
        _tpl_name, _tpl_bytes = _mgmt_files["template"]
        _re_export = []
        for _it in _mgmt_items:
            _cm4 = _mgmt_cp.get(_it.coupang_option_id)
            _own_bc = _cm4.wms_barcode if _cm4 else None
            _wp4 = _mgmt_wms.get(_own_bc) if _own_bc else None
            _shl4 = _wp4.shelf_life_days if _wp4 else None
            _exp4, _man4 = dates_from_batch(_it.wms_short_expiry, _shl4) if _it.wms_short_expiry else default_expiry_dates(_shl4)
            _re_export.append(ExportItem(
                coupang_option_id=_it.coupang_option_id,
                inbound_qty=_it.inbound_qty_final,
                shelf_life_days=_shl4, expiry_date=_exp4,
                manufacture_date=_man4, wms_barcode=_own_bc,
                product_name=_it.product_name,
            ))
        try:
            _re_xlsx, _re_miss = fill_coupang_template(io.BytesIO(_tpl_bytes), _re_export, delete_non_target=True)
            st.download_button("쿠팡 입고생성 파일 다운로드", data=_re_xlsx,
                file_name=f"generated_excel_{date.today().isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary")
            if _re_miss:
                st.warning(f"⚠️ {len(_re_miss)}건 누락 (쿠팡 양식에 없는 옵션ID)")
        except Exception as e:
            st.error(f"양식 생성 실패: {e}")

    # === SecondaryItem + PalletAssignment 구축 ===
    _sec_items: list[SecondaryItem] = []
    for _it in _mgmt_items:
        _cm5 = _mgmt_cp.get(_it.coupang_option_id)
        _own5 = _cm5.wms_barcode if _cm5 else None
        _wp5 = _mgmt_wms.get(_own5) if _own5 else None
        _pbc5, _uq5 = _resolve_parent_barcode(_cm5, _mgmt_wms) if _cm5 else (None, 1)
        _pwp5 = _mgmt_wms.get(_pbc5) if _pbc5 else None
        _wg5 = (_wp5.weight_g if _wp5 and _wp5.weight_g else 0) or (_pwp5.weight_g if _pwp5 and _pwp5.weight_g else 0)
        _shl5 = (_wp5.shelf_life_days if _wp5 else None) or (_pwp5.shelf_life_days if _pwp5 else None)
        _mfg5 = None
        _exp5 = _it.wms_short_expiry
        if _exp5 and _shl5:
            _mfg5 = _exp5 - timedelta(days=int(_shl5) - 1)
        _cpn5 = _cm5.product_name if _cm5 else (_it.product_name or "")
        _cpo5 = _cm5.option_name if _cm5 else _it.option_name
        _wmsn5 = (_wp5.product_name if _wp5 and _wp5.product_name else None) or (_pwp5.product_name if _pwp5 and _pwp5.product_name else None)
        _bq5 = _it.box_qty or 1
        _boxes5 = (_it.inbound_qty_final or 0) // max(_bq5, 1)
        _sec_items.append(SecondaryItem(
            coupang_option_id=_it.coupang_option_id,
            sku_id=_cm5.sku_id if _cm5 else None,
            coupang_product_id=_cm5.coupang_product_id if _cm5 else None,
            product_name=_cpn5, option_name=_cpo5, wms_product_name=_wmsn5,
            own_wms_barcode=_own5,
            coupang_barcode=_cm5.coupang_barcode if _cm5 else None,
            parent_wms_barcode=_pbc5, unit_qty=_uq5,
            inbound_qty=_it.inbound_qty_final or 0,
            box_qty=_bq5, boxes=_boxes5,
            weight_g=int(_wg5 or 0), expiry_date=_exp5,
            manufacture_date=_mfg5, shelf_life_days=int(_shl5) if _shl5 else None,
        ))

    from lib.pallet_assign import PalletAssignment, PalletEntry
    # 저장된 pallet_no 가 있으면 그대로, 없으면 박스수 기반 재할당
    _has_pallet_no = any(_it.pallet_no for _it in _mgmt_items)
    if _has_pallet_no:
        _pallet_map: dict[int, list[PalletEntry]] = {}
        for _it in _mgmt_items:
            pn = _it.pallet_no or 1
            _boxes_it = (_it.inbound_qty_final or 0) // max(_it.box_qty or 1, 1)
            if _boxes_it <= 0:
                continue
            _pallet_map.setdefault(pn, []).append(
                PalletEntry(key=_it.coupang_option_id, name=_it.product_name or "", boxes=_boxes_it)
            )
        _pa = PalletAssignment(
            pallets=[_pallet_map[k] for k in sorted(_pallet_map.keys())],
            total_boxes=sum(e.boxes for p in _pallet_map.values() for e in p),
            pallet_count=len(_pallet_map),
        )
    else:
        _pa_items = [
            PA_PalletItem(
                key=_it.coupang_option_id,
                name=_it.product_name or "",
                boxes=(_it.inbound_qty_final or 0) // max(_it.box_qty or 1, 1),
            )
            for _it in _mgmt_items
            if (_it.inbound_qty_final or 0) // max(_it.box_qty or 1, 1) > 0
        ]
        _pa = pa_assign_pallets(_pa_items, pallet_size=cfg.pallet_size_boxes)

    # === ④ 쿠팡 입고생성 결과물 검수 ===
    _step4_label = "④ 쿠팡 입고생성 결과물 검수"
    if _mgmt_status in ("verified", "completed"):
        _step4_label += " ✅"
    st.subheader(_step4_label)
    st.caption(
        "쿠팡 입고생성 결과물을 업로드 해주세요. "
        "바코드 라벨 다운로드 시 소비기한 표기 체크는 필수이며, 번들 상품만 적용합니다."
    )

    _STEP4_GUIDE_ROWS = [
        ("바코드 라벨",
         "sku-barcode-labels-YYYYMMDD_hhmmss.pdf",
         "쿠팡Wing &gt; 로켓그로스 &gt; 입고관리 &gt; 해당 회차의 [상세보기] &gt; 바코드 인쇄"
         "<br>(번들 상품만, 소비기한 표기 체크 필수)"),
        ("동봉 문서",
         "물류동봉문서_YYYYMMDD_hhmmss.pdf",
         "쿠팡Wing &gt; 로켓그로스 &gt; 입고관리 &gt; 해당 회차의 [상세보기] &gt; 동봉문서 인쇄"),
        ("부착 문서",
         "물류부착문서_YYYYMMDD_hhmmss.pdf",
         "쿠팡Wing &gt; 로켓그로스 &gt; 입고관리 &gt; 해당 회차의 [상세보기] &gt; 부착문서 인쇄"),
    ]

    def _render_step4_guide(uploaded: dict[str, bool]) -> str:
        body = ""
        for label, fname_example, path in _STEP4_GUIDE_ROWS:
            mark = "✅" if uploaded.get(label) else ""
            body += (
                f"<tr><td>{label}</td>"
                f'<td><code style="font-size:0.85em;">{fname_example}</code></td>'
                f"<td>{path}</td>"
                f'<td style="width:90px; text-align:center; white-space:nowrap;">{mark}</td>'
                f"</tr>"
            )
        return (
            '<table style="border-collapse: collapse; width: 100%;">'
            '<thead><tr>'
            '<th style="text-align:left;">구분</th>'
            '<th style="text-align:left;">파일명 예시</th>'
            '<th style="text-align:left;">경로</th>'
            '<th style="width:90px; text-align:center; white-space:nowrap;">업로드</th>'
            '</tr></thead>'
            f'<tbody>{body}</tbody>'
            '</table>'
        )

    _step4_guide_ph = st.empty()
    _step4_guide_ph.markdown(_render_step4_guide({}), unsafe_allow_html=True)

    _pdf_up = st.file_uploader(
        "쿠팡 입고생성 결과물 파일(PDF 3개)를 업로드",
        type=["pdf"], accept_multiple_files=True,
        key=f"mgmt_pdf_{_selected_plan_id}",
    )

    _label_pdf = _attach_pdf = _invoice_pdf = None
    for f in (_pdf_up or []):
        if "label" in f.name.lower() or "barcode" in f.name.lower():
            _label_pdf = f
        elif "물류부착" in f.name or "부착문서" in f.name:
            _attach_pdf = f
        elif "물류동봉" in f.name or "동봉문서" in f.name:
            _invoice_pdf = f

    # DB fallback
    if not _label_pdf and "label_pdf" in _mgmt_files:
        _n, _b = _mgmt_files["label_pdf"]
        _label_pdf = io.BytesIO(_b)
        _label_pdf.name = _n
    if not _attach_pdf and "attach_pdf" in _mgmt_files:
        _n, _b = _mgmt_files["attach_pdf"]
        _attach_pdf = io.BytesIO(_b)
        _attach_pdf.name = _n
    if not _invoice_pdf and "invoice_pdf" in _mgmt_files:
        _n, _b = _mgmt_files["invoice_pdf"]
        _invoice_pdf = io.BytesIO(_b)
        _invoice_pdf.name = _n

    _mv_blob = _mgmt_plan.movement_template_blob
    _mv_fname = _mgmt_plan.movement_template_filename

    # 가이드 테이블 업로드 칸 갱신
    _step4_guide_ph.markdown(
        _render_step4_guide({
            "바코드 라벨": bool(_label_pdf),
            "동봉 문서": bool(_invoice_pdf),
            "부착 문서": bool(_attach_pdf),
        }),
        unsafe_allow_html=True,
    )

    if _label_pdf and _attach_pdf:
        _lb = _label_pdf.getvalue() if hasattr(_label_pdf, 'getvalue') else _label_pdf.read()
        _ab = _attach_pdf.getvalue() if hasattr(_attach_pdf, 'getvalue') else _attach_pdf.read()
        _lname = getattr(_label_pdf, 'name', 'label.pdf')
        _aname = getattr(_attach_pdf, 'name', 'attach.pdf')

        # PDF 저장
        _new_pdfs: dict[str, tuple[str, bytes]] = {}
        if "label_pdf" not in _mgmt_files:
            _new_pdfs["label_pdf"] = (_lname, _lb)
        if "attach_pdf" not in _mgmt_files:
            _new_pdfs["attach_pdf"] = (_aname, _ab)
        _ib = None
        if _invoice_pdf:
            _ib = _invoice_pdf.getvalue() if hasattr(_invoice_pdf, 'getvalue') else _invoice_pdf.read()
            _iname = getattr(_invoice_pdf, 'name', 'invoice.pdf')
            if "invoice_pdf" not in _mgmt_files:
                _new_pdfs["invoice_pdf"] = (_iname, _ib)
        if _new_pdfs:
            _save_plan_files(_selected_plan_id, _new_pdfs)

        _labels = parse_barcode_labels(_lb)
        _attachment = parse_attachment_doc(_ab)
        _invoice = parse_invoice_doc(_ib) if _ib else None

        # PlannedSku
        _planned: list[PlannedSku] = []
        for _it in _mgmt_items:
            _cm6 = _mgmt_cp.get(_it.coupang_option_id)
            _own6 = _cm6.wms_barcode if _cm6 else None
            _cbc6 = _cm6.coupang_barcode if _cm6 else None
            _pbc6, _uq6 = _resolve_parent_barcode(_cm6, _mgmt_wms) if _cm6 else (None, 1)
            _wp6 = _mgmt_wms.get(_own6) if _own6 else None
            _pwp6 = _mgmt_wms.get(_pbc6) if _pbc6 else None
            _shl6 = (_wp6.shelf_life_days if _wp6 else None) or (_pwp6.shelf_life_days if _pwp6 else None)
            _bq6 = _it.box_qty or 1
            _boxes6 = (_it.inbound_qty_final or 0) // max(_bq6, 1)
            _emfg6 = None
            if _it.wms_short_expiry and _shl6:
                _emfg6 = _it.wms_short_expiry - timedelta(days=int(_shl6) - 1)
            _planned.append(PlannedSku(
                coupang_option_id=_it.coupang_option_id,
                sku_id=_cm6.sku_id if _cm6 else None,
                product_name=_cm6.product_name if _cm6 else _it.product_name,
                option_name=_cm6.option_name if _cm6 else _it.option_name,
                own_wms_barcode=_own6,
                parent_wms_barcode=_pbc6, unit_qty=_uq6,
                coupang_barcode=_cbc6,
                inbound_qty=_it.inbound_qty_final or 0,
                box_qty=_bq6, boxes=_boxes6,
                expects_label=False,
                expected_attached_barcode=None,
                expected_expiry=_it.wms_short_expiry,
                expected_manufacture=_emfg6,
            ))

        # 중복 체크
        _dup = False
        if _attachment.milkrun_id:
            with get_session() as _ds:
                _dups = _ds.execute(select(CoupangResultLog).where(
                    CoupangResultLog.milkrun_id == _attachment.milkrun_id,
                    CoupangResultLog.company_name == _mgmt_company,
                )).scalars().all()
                _existing_ids = {d.plan_id for d in _dups}
                if _dups and _selected_plan_id not in _existing_ids:
                    _dup = True
                    st.warning(f"⚠️ 밀크런 ID {_attachment.milkrun_id} 는 이미 처리된 이력이 있습니다.")

        # 검수: 재고이동건 파일이 있으면 번들 합계로 자체 검증 (파일 단순 존재 체크)
        _mvt_total = None
        if _mv_blob:
            _mvt_total = sum(
                s.inbound_qty for s in _planned
                if s.unit_qty and s.unit_qty >= 2 and s.inbound_qty > 0
            )
        _report = verify(
            planned_skus=_planned,
            labels=_labels,
            attachment=_attachment,
            pallet_assignment=_pa,
            duplicate_check=_dup,
            movement_inbound_total=_mvt_total,
            invoice=_invoice,
        )
        if _report.overall == "ok":
            st.success("✅ 검수 통과")
        elif _report.overall == "warning":
            st.warning("⚠️ 일부 항목 확인 필요")
        else:
            st.error("❌ 검수 실패")

        _icon = {"ok": "✅", "warning": "⚠️", "fail": "❌"}
        for _ck in _report.checks:
            _lbl2 = f"{_icon.get(_ck.status, '•')} **{_ck.name}**"
            if _ck.expected is not None and _ck.actual is not None:
                _lbl2 += f" — {_ck.actual} (예상 {_ck.expected})"
            elif _ck.actual is not None:
                _lbl2 += f" — {_ck.actual}"
            st.markdown(_lbl2)
            if _ck.detail:
                st.caption(_ck.detail)
            if _ck.items:
                with st.expander(f"세부 {len(_ck.items)}건"):
                    st.dataframe(pd.DataFrame(_ck.items), use_container_width=True, hide_index=True)

        # === ④ 검수 끝 (이후 ⑤·⑥에서 사용할 변수 미리 준비) ===
        _order_base = (_invoice.order_id if _invoice and _invoice.order_id else _attachment.milkrun_id) or ""
        _fc = _attachment.fc_name or _mgmt_plan.fc_name or "FC"
        _arr = _attachment.arrival_date or _mgmt_plan.arrival_date or date.today()
        _yymmdd = _arr.strftime("%y%m%d")
        _datesuf = _arr.strftime("%Y%m%d")
        _yyyymm = _arr.strftime("%Y_%m월")

        # 발주 확정 — ④의 종결 액션
        if _mgmt_status == "draft":
            if st.button("✅ 검수 완료 — 발주 확정", type="primary", use_container_width=True, disabled=_dup, key="mgmt_confirm"):
                try:
                    with get_session() as _s4:
                        _po = _s4.get(InboundPlan, _selected_plan_id)
                        _po.status = "verified"
                        _po.fc_name = _fc
                        _po.arrival_date = _arr
                        _po.milkrun_id = _attachment.milkrun_id
                        _po.total_pallets = _pa.pallet_count
                        _po.verified_at = datetime.now(timezone.utc)
                        _po.confirmed_at = datetime.now(timezone.utc)
                        _ibo = {it.coupang_option_id: it for it in _mgmt_items}
                        for _pi2, _pal2 in enumerate(_pa.pallets, start=1):
                            for _en in _pal2:
                                _dbi = _ibo.get(_en.key)
                                if _dbi:
                                    _sk = next((s for s in _planned if s.coupang_option_id == _en.key), None)
                                    if _sk:
                                        _cm7 = _mgmt_cp.get(_sk.coupang_option_id)
                                        _bc7 = (_cm7.coupang_barcode if _cm7 and _cm7.coupang_barcode and _cm7.coupang_barcode.startswith("S0") else _sk.own_wms_barcode)
                                        _bt7 = "쿠팡바코드" if (_cm7 and _cm7.coupang_barcode and _cm7.coupang_barcode.startswith("S0")) else "88코드"
                                        _dbi.pallet_no = _pi2
                                        _dbi.barcode_attached = _bc7
                                        _dbi.barcode_type = _bt7
                        _tb = sum(s.boxes for s in _planned)
                        _s4.add(CoupangResultLog(
                            company_name=_mgmt_company,
                            milkrun_id=_attachment.milkrun_id or "",
                            fc_name=_fc, arrival_date=_arr,
                            total_pallets=_pa.pallet_count, total_boxes=_tb,
                            total_skus=len([s for s in _planned if s.boxes > 0]),
                            plan_id=_selected_plan_id,
                            label_filename=_lname, attachment_filename=_aname,
                        ))
                        _s4.commit()
                    st.success(f"✅ 발주 #{_selected_plan_id} 확정 완료")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"확정 실패: {e}")

        # === ⑤ 물류센터 전달 파일 생성 ===
        _step5_label = "⑤ 물류센터 전달 파일 생성"
        if _mgmt_status in ("verified", "completed"):
            _step5_label += " ✅"
        st.subheader(_step5_label)

        _dc = st.columns(3)
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
                milkrun_request_id=_order_base, pallet_size=cfg.pallet_size_boxes)
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

        # PDF 리네임 (물류센터 전달 파일에 포함)
        _dpc = st.columns(3)
        if _ib:
            with _dpc[0]:
                st.download_button("📥 물류동봉문서(거래명세서)", data=_ib,
                    file_name=f"밀크런_물류동봉문서(거래명세서)_{_fc}_{_datesuf}.pdf", mime="application/pdf",
                    use_container_width=True, type="primary")
        with _dpc[1]:
            st.download_button("📥 제품 바코드라벨", data=_lb,
                file_name=f"제품 바코드라벨_{_fc}_{_datesuf}.pdf", mime="application/pdf",
                use_container_width=True, type="primary")
        with _dpc[2]:
            st.download_button("📥 물류부착문서(팔레트부착)", data=_ab,
                file_name=f"밀크런_물류부착문서1 (팔레트부착문서)_{_fc}_{_datesuf}.pdf", mime="application/pdf",
                use_container_width=True, type="primary")

        # === ⑥ 이지어드민 재고 차감 ===
        _step6_label = "⑥ 이지어드민 재고 차감"
        if _mgmt_status == "completed":
            _step6_label += " ✅"
        st.subheader(_step6_label)
        _ea = st.columns(3)
        try:
            _ord = build_order_form(_sec_items, _fc, str(_order_base).strip(), pallet_assignment=_pa)
            with _ea[0]:
                st.download_button("📥 발주서양식", data=_ord,
                    file_name=f"밀크런재고차감_로켓그로스({cfg.default_company_name}커머스)발주서양식_{_datesuf}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True, type="primary")
        except Exception as e:
            with _ea[0]:
                st.error(str(e))
    else:
        st.info("라벨 PDF와 물류부착문서 PDF를 업로드하세요.")

    # === ⑥ 이지어드민 재고 차감 — 확장주문검색·배송일괄·송장 (발주 확정 후) ===
    if _mgmt_status in ("verified", "completed") and _label_pdf and _attach_pdf:
        st.markdown("#### 확장주문검색 / 배송일괄 처리 / 송장 업로드")
        _order_base3 = (_invoice.order_id if _invoice and _invoice.order_id else None) or (_mgmt_plan.milkrun_id or "")

        _os_uploaded = "order_search" in _mgmt_files
        _os_guide_html = (
            '<table style="border-collapse: collapse; width: 100%;">'
            '<thead><tr>'
            '<th style="text-align:left;">구분</th>'
            '<th style="text-align:left;">취합 경로</th>'
            '<th style="width:90px; text-align:center; white-space:nowrap;">취합여부</th>'
            '</tr></thead>'
            '<tbody><tr>'
            '<td>확장주문검색 파일</td>'
            '<td>이지어드민 &gt; 주문관리 &gt; 확장주문검색2 &gt; 판매처 - 로켓그로스(서현커머스) &gt; 다운로드 포맷 [내뉴]발주서 &gt; 다운로드</td>'
            f'<td style="width:90px; text-align:center; white-space:nowrap;">{"✅" if _os_uploaded else ""}</td>'
            '</tr></tbody>'
            '</table>'
        )
        _os_guide_ph = st.empty()
        _os_guide_ph.markdown(_os_guide_html, unsafe_allow_html=True)

        _os_file = st.file_uploader(
            "확장주문검색 파일 업로드",
            type=["xls", "xlsx"],
            key=f"mgmt_os_{_selected_plan_id}",
            label_visibility="collapsed",
        )
        if not _os_file and "order_search" in _mgmt_files:
            _osn, _osb = _mgmt_files["order_search"]
            _os_file = io.BytesIO(_osb)
            _os_file.name = _osn

        if _os_file is not None:
            _os_bytes = _os_file.getvalue() if hasattr(_os_file, 'getvalue') else _os_file.read()
            _os_name = getattr(_os_file, 'name', 'order_search.xls')
            if "order_search" not in _mgmt_files:
                _save_plan_files(_selected_plan_id, {"order_search": (_os_name, _os_bytes)})
            # 방금 업로드된 경우도 가이드 테이블에 ✅ 반영
            _os_uploaded = True
            _os_guide_ph.markdown(
                _os_guide_html.replace(
                    'style="width:90px; text-align:center; white-space:nowrap;"></td>',
                    'style="width:90px; text-align:center; white-space:nowrap;">✅</td>',
                ),
                unsafe_allow_html=True,
            )

            try:
                _os_rows = parse_order_search_file(_os_bytes)
            except Exception as e:
                st.error(f"파일 파싱 실패: {e}")
                _os_rows = []

            if _os_rows:
                st.info(f"파싱 완료: {len(_os_rows)}건")
                _inv_qty_by_sku = None
                if _invoice and _invoice.items:
                    _inv_qty_by_sku = {str(x.sku_id): int(x.confirmed_qty) for x in _invoice.items if x.sku_id}
                _chk = validate_order_search(
                    _os_rows, _sec_items, str(_order_base3).strip(),
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

                _dsuf3 = (_mgmt_plan.arrival_date or date.today()).strftime("%Y%m%d")
                _tc = st.columns(2)
                try:
                    _bulk = build_shipping_bulk_form(_os_rows)
                    with _tc[0]:
                        st.download_button("📥 배송일괄처리양식", data=_bulk,
                            file_name=f"밀크런재고차감_배송일괄처리양식_{_dsuf3}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True, type="primary",
                            disabled=(_chk.status != "ok"))
                except Exception as e:
                    with _tc[0]:
                        st.error(str(e))
                try:
                    _inv3 = build_invoice_upload_form(_os_rows)
                    with _tc[1]:
                        st.download_button("📥 송장업로드양식", data=_inv3,
                            file_name=f"밀크런재고차감_송장업로드양식_{_dsuf3}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True, type="primary",
                            disabled=(_chk.status != "ok"))
                except Exception as e:
                    with _tc[1]:
                        st.error(str(e))

                if _mgmt_status != "completed" and _chk.status == "ok":
                    if st.button("✅ 재고차감 완료", type="primary", use_container_width=True,
                                 key=f"mgmt_deduct_{_selected_plan_id}"):
                        try:
                            with get_session() as _s5:
                                _po2 = _s5.get(InboundPlan, _selected_plan_id)
                                _po2.status = "completed"
                                _s5.commit()
                            st.success(f"✅ 발주 #{_selected_plan_id} 재고차감 완료")
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"상태 업데이트 실패: {e}")
    elif _mgmt_status == "draft":
        st.caption("ℹ️ 검수 PDF 업로드 후 발주 확정하면 재고차감 단계가 활성화됩니다.")
