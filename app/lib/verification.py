"""쿠팡 결과 검수 엔진.

입력:
  - InboundPlan (draft) 의 확정 SKU 데이터
  - 라벨 PDF 파싱 결과 ({barcode: LabelInfo})
  - 물류부착문서 PDF 파싱 결과 (AttachmentMeta)
  - 제품 마스터 (CoupangProduct + WmsProduct)
  - 팔레트 배분 결과 (PalletAssignment)

출력:
  - VerificationReport (각 항목별 ok/fail/warning + 세부 사항)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from lib.coupang_result import AttachmentMeta, InvoiceMeta, LabelInfo, name_similarity
from lib.pallet_assign import PalletAssignment


@dataclass
class CheckItem:
    """단일 검수 항목."""

    name: str                      # "팔레트수 일치"
    status: str                    # "ok" | "warning" | "fail"
    expected: Any = None
    actual: Any = None
    detail: str = ""
    items: list[dict[str, Any]] = field(default_factory=list)  # 세부 SKU 목록


@dataclass
class VerificationReport:
    overall: str = "ok"             # ok | warning | fail
    checks: list[CheckItem] = field(default_factory=list)
    label_issues: list[dict[str, Any]] = field(default_factory=list)
    duplicate_milkrun: bool = False
    duplicate_info: str | None = None

    def add(self, check: CheckItem) -> None:
        self.checks.append(check)
        # 전체 상태 갱신: fail > warning > ok
        if check.status == "fail" and self.overall != "fail":
            self.overall = "fail"
        elif check.status == "warning" and self.overall == "ok":
            self.overall = "warning"


@dataclass
class PlannedSku:
    """검수 대상 SKU 정보 (Plan 에서 가져옴)."""

    coupang_option_id: int
    product_name: str | None
    option_name: str | None
    inbound_qty: int                  # 확정 수량 (낱개)
    box_qty: int                      # 박스 입수량
    boxes: int                        # 박스 수
    own_wms_barcode: str | None       # SKU 자신의 wms_barcode
    parent_wms_barcode: str | None
    unit_qty: int                     # 1개입=1, 2개입=2 ...
    coupang_barcode: str | None       # 쿠팡 부착바코드 (S00...)
    expects_label: bool               # 라벨 출력 대상 여부 (번들 또는 쿠팡바코드 단품)
    sku_id: int | None                 # 쿠팡 SKU ID (거래명세서 상품번호 매칭용)
    expected_attached_barcode: str | None  # 부착바코드 (쿠팡바코드 우선, 없으면 wms)
    expected_expiry: date | None      # 우리가 입력한 소비기한 (배치 선택 결과)
    expected_manufacture: date | None  # 제조일자 (소비기한 - 유통기한일수 + 1)


def derive_attached_barcode(sku: PlannedSku) -> tuple[str | None, str]:
    """이 SKU 에 출력될 부착바코드와 종류 반환.

    Returns: (barcode, type)  type ∈ {'쿠팡바코드', '88코드'}
    """
    # S00 으로 시작하면 쿠팡바코드
    if sku.coupang_barcode and sku.coupang_barcode.startswith("S0"):
        return sku.coupang_barcode, "쿠팡바코드"
    # 그렇지 않으면 88코드 (wms_barcode)
    return sku.own_wms_barcode, "88코드"


def is_label_expected(sku: PlannedSku) -> bool:
    """라벨 PDF 출력 대상인지 판정.

    출력 대상:
      1. 번들 (unit_qty >= 2)
      2. 단품(unit_qty=1) 이지만 쿠팡바코드(S00) 사용
    """
    if sku.unit_qty and sku.unit_qty >= 2:
        return True
    bc, kind = derive_attached_barcode(sku)
    return kind == "쿠팡바코드"


def verify(
    planned_skus: list[PlannedSku],
    labels: dict[str, LabelInfo],
    attachment: AttachmentMeta,
    pallet_assignment: PalletAssignment,
    duplicate_check: bool = False,
    duplicate_info: str | None = None,
    name_similarity_threshold: float = 0.6,
    movement_inbound_total: int | None = None,
    invoice: InvoiceMeta | None = None,
) -> VerificationReport:
    """검수 수행.

    Args:
        planned_skus: 확정 발주 SKU 리스트 (확정수량 > 0 만)
        labels: 라벨 PDF 파싱 결과
        attachment: 물류부착문서 PDF 메타
        pallet_assignment: 팔레트 배분 결과
        duplicate_check: 중복 milkrun_id 감지 여부
        duplicate_info: 중복 시 안내 문구

    Returns: VerificationReport
    """
    report = VerificationReport()

    # ----- 0. 중복 milkrun_id -----
    if duplicate_check:
        report.duplicate_milkrun = True
        report.duplicate_info = duplicate_info or "이미 검수된 밀크런ID 입니다."
        report.add(
            CheckItem(
                name="중복/과거 검사",
                status="fail",
                detail=report.duplicate_info,
            )
        )
    else:
        report.add(CheckItem(name="중복/과거 검사", status="ok", detail="신규 밀크런 ID"))

    # ----- 1. 팔레트수 일치 -----
    expected_pallets = pallet_assignment.pallet_count
    actual_pallets = attachment.total_pallets
    if expected_pallets == actual_pallets:
        report.add(
            CheckItem(
                name="팔레트수 일치",
                status="ok",
                expected=expected_pallets,
                actual=actual_pallets,
            )
        )
    else:
        report.add(
            CheckItem(
                name="팔레트수 일치",
                status="fail",
                expected=expected_pallets,
                actual=actual_pallets,
                detail=f"우리 계산 {expected_pallets} vs 쿠팡 결과 {actual_pallets}",
            )
        )

    # ----- 2. 총 박스수 / 총 수량 -----
    expected_boxes = sum(s.boxes for s in planned_skus)
    expected_total_qty = sum(s.inbound_qty for s in planned_skus)
    report.add(
        CheckItem(
            name="총 박스수",
            status="ok",
            expected=expected_boxes,
            actual=expected_boxes,
            detail=f"확정 박스 합계 {expected_boxes}",
        )
    )

    # ----- 3. 라벨 vs 발주 수량 매칭 (라벨 출력 대상 SKU만) -----
    expected_labels: dict[str, PlannedSku] = {}  # 부착바코드 → sku
    missing_label_skus: list[dict[str, Any]] = []
    for sku in planned_skus:
        if not is_label_expected(sku):
            continue
        attached_bc, _ = derive_attached_barcode(sku)
        if attached_bc:
            expected_labels[attached_bc] = sku

    # 누락 검증: 우리가 기대하는 라벨이 PDF 에 없음
    missing_in_pdf = []
    for bc, sku in expected_labels.items():
        if bc not in labels:
            missing_in_pdf.append(
                {
                    "barcode": bc,
                    "product_name": sku.product_name,
                    "option_name": sku.option_name,
                    "inbound_qty": sku.inbound_qty,
                }
            )

    if missing_in_pdf:
        report.add(
            CheckItem(
                name="라벨 누락",
                status="fail",
                detail=f"{len(missing_in_pdf)}개 SKU 의 라벨이 PDF에 없음",
                items=missing_in_pdf,
            )
        )
    else:
        report.add(CheckItem(name="라벨 누락", status="ok", detail=f"{len(expected_labels)}개 SKU 모두 발견"))

    # 추가 검증: PDF 에 우리 발주 외 라벨이 들어 있음
    extra_in_pdf = []
    for bc in labels.keys():
        if bc not in expected_labels:
            extra_in_pdf.append(
                {
                    "barcode": bc,
                    "label_count": labels[bc].count,
                    "label_name": labels[bc].raw_name,
                }
            )
    if extra_in_pdf:
        report.add(
            CheckItem(
                name="라벨 추가(잘못 들어감)",
                status="warning",
                detail=f"{len(extra_in_pdf)}개 라벨이 발주에 없음",
                items=extra_in_pdf,
            )
        )
    else:
        report.add(CheckItem(name="라벨 추가(잘못 들어감)", status="ok"))

    # ----- 4. 라벨 개수 = 확정 수량 일치 -----
    qty_mismatches = []
    for bc, sku in expected_labels.items():
        if bc not in labels:
            continue  # 누락은 위에서 처리
        label_count = labels[bc].count
        if label_count != sku.inbound_qty:
            qty_mismatches.append(
                {
                    "barcode": bc,
                    "product_name": sku.product_name,
                    "expected": sku.inbound_qty,
                    "actual": label_count,
                    "diff": label_count - sku.inbound_qty,
                }
            )
    if qty_mismatches:
        report.add(
            CheckItem(
                name="라벨 수량 일치",
                status="fail",
                detail=f"{len(qty_mismatches)}개 SKU 수량 불일치",
                items=qty_mismatches,
            )
        )
    else:
        report.add(CheckItem(name="라벨 수량 일치", status="ok", detail=f"{len(expected_labels)}건 모두 일치"))

    # ----- 5. 소비기한 일치 -----
    expiry_mismatches = []
    for bc, sku in expected_labels.items():
        if bc not in labels:
            continue
        label_expiry = labels[bc].expiry
        if sku.expected_expiry and label_expiry and label_expiry != sku.expected_expiry:
            expiry_mismatches.append(
                {
                    "barcode": bc,
                    "product_name": sku.product_name,
                    "expected": sku.expected_expiry.isoformat(),
                    "actual": label_expiry.isoformat(),
                }
            )
    if expiry_mismatches:
        report.add(
            CheckItem(
                name="소비기한 일치",
                status="warning",
                detail=f"{len(expiry_mismatches)}건 소비기한 불일치",
                items=expiry_mismatches,
            )
        )
    else:
        report.add(CheckItem(name="소비기한 일치", status="ok"))

    # ----- 6. 제품명 fuzzy 매칭 (낮은 유사도만 경고) -----
    name_low = []
    for bc, sku in expected_labels.items():
        if bc not in labels:
            continue
        our_name = " ".join(filter(None, [sku.product_name, sku.option_name]))
        label_name = labels[bc].raw_name or ""
        sim = name_similarity(our_name, label_name)
        if sim < name_similarity_threshold:
            name_low.append(
                {
                    "barcode": bc,
                    "our_name": our_name,
                    "label_name": label_name,
                    "similarity": round(sim, 2),
                }
            )
    if name_low:
        report.add(
            CheckItem(
                name="제품명 매칭",
                status="warning",
                detail=f"{len(name_low)}건 유사도 낮음 (수동 확인)",
                items=name_low,
            )
        )
    else:
        report.add(CheckItem(name="제품명 매칭", status="ok"))

    # ----- 7. 재고이동건 입고수량 합 = 번들작업표 수량 합 -----
    bundle_skus = [s for s in planned_skus if s.unit_qty and s.unit_qty >= 2 and s.inbound_qty > 0]
    bundle_total_qty = sum(s.inbound_qty for s in bundle_skus)

    if movement_inbound_total is not None:
        # 재고이동건 D열 합과 번들작업표 수량합 직접 비교
        if movement_inbound_total == bundle_total_qty:
            report.add(
                CheckItem(
                    name="재고이동건↔번들작업표 수량 일치",
                    status="ok",
                    expected=bundle_total_qty,
                    actual=movement_inbound_total,
                    detail=f"번들 {len(bundle_skus)}개 SKU, 합계 {bundle_total_qty}",
                )
            )
        else:
            report.add(
                CheckItem(
                    name="재고이동건↔번들작업표 수량 일치",
                    status="fail",
                    expected=bundle_total_qty,
                    actual=movement_inbound_total,
                    detail=f"번들작업표 합 {bundle_total_qty} vs 재고이동건 입고합 {movement_inbound_total}",
                )
            )
    elif bundle_total_qty > 0:
        report.add(
            CheckItem(
                name="재고이동건↔번들작업표 수량 일치",
                status="ok",
                expected=bundle_total_qty,
                actual=bundle_total_qty,
                detail=f"번들 {len(bundle_skus)}개 SKU, 입고수량 합 {bundle_total_qty} (재고이동건 파일 미제공으로 자체검증)",
            )
        )
    else:
        report.add(
            CheckItem(
                name="재고이동건↔번들작업표 수량 일치",
                status="ok",
                detail="번들 SKU 없음 (단품만)",
            )
        )

    # ----- 8. 거래명세서(물류동봉문서) 검증 -----
    if invoice and invoice.items:
        # 8a. 총 확정수량 일치
        our_total = sum(s.inbound_qty for s in planned_skus)
        inv_total = invoice.total_confirmed_qty
        if inv_total is not None:
            if our_total == inv_total:
                report.add(
                    CheckItem(name="거래명세서 총수량 일치", status="ok",
                              expected=our_total, actual=inv_total)
                )
            else:
                report.add(
                    CheckItem(name="거래명세서 총수량 일치", status="fail",
                              expected=our_total, actual=inv_total,
                              detail=f"우리 {our_total} vs 거래명세서 {inv_total}")
                )

        # 8b. 팔레트수 일치
        if invoice.pallet_count is not None:
            if invoice.pallet_count == pallet_assignment.pallet_count:
                report.add(
                    CheckItem(name="거래명세서 팔레트수 일치", status="ok",
                              expected=pallet_assignment.pallet_count,
                              actual=invoice.pallet_count)
                )
            else:
                report.add(
                    CheckItem(name="거래명세서 팔레트수 일치", status="fail",
                              expected=pallet_assignment.pallet_count,
                              actual=invoice.pallet_count)
                )

        # 8c. SKU별 수량 일치
        # 거래명세서 바코드 → item 매핑
        inv_by_barcode = {it.barcode: it for it in invoice.items if it.barcode}
        # sku_id(상품번호) 로도 매핑 (바코드 불일치 시 폴백)
        inv_by_sku = {it.sku_id: it for it in invoice.items if it.sku_id}
        # planned: coupang_barcode AND wms_barcode 양쪽 모두 등록
        planned_by_barcode: dict[str, PlannedSku] = {}
        for s in planned_skus:
            if s.coupang_barcode:
                planned_by_barcode[s.coupang_barcode] = s
            if s.own_wms_barcode and s.own_wms_barcode not in planned_by_barcode:
                planned_by_barcode[s.own_wms_barcode] = s

        qty_mismatches = []
        for bc, inv_item in inv_by_barcode.items():
            planned = planned_by_barcode.get(bc)
            if not planned:
                continue
            if inv_item.confirmed_qty != planned.inbound_qty:
                qty_mismatches.append({
                    "barcode": bc,
                    "product": inv_item.product_name or "",
                    "우리수량": planned.inbound_qty,
                    "거래명세서수량": inv_item.confirmed_qty,
                })
        if qty_mismatches:
            report.add(
                CheckItem(name="거래명세서 SKU별 수량 일치", status="fail",
                          detail=f"{len(qty_mismatches)}건 불일치", items=qty_mismatches)
            )
        else:
            report.add(
                CheckItem(name="거래명세서 SKU별 수량 일치", status="ok",
                          detail=f"{len(inv_by_barcode)}건 매칭 완료")
            )

        # 8d. SKU 누락/추가 — 바코드 OR sku_id 로 매칭 시도
        # 각 planned SKU 가 거래명세서에 존재하는지
        # 매칭 키: coupang_barcode → wms_barcode → sku_id (3중 폴백)
        # 거래명세서의 상품번호 = SKU ID
        inv_by_sku = {str(it.sku_id): it for it in invoice.items if it.sku_id}

        _planned_matched: set[int] = set()  # 매칭 성공한 option_id
        _inv_matched: set[str] = set()  # 매칭된 invoice barcode
        for s in planned_skus:
            matched = False
            # 1순위: 바코드 매칭
            for bc in [s.coupang_barcode, s.own_wms_barcode]:
                if bc and bc in inv_by_barcode:
                    _planned_matched.add(s.coupang_option_id)
                    _inv_matched.add(bc)
                    matched = True
                    break
            # 2순위: SKU ID 매칭 (거래명세서의 상품번호 = SKU ID)
            if not matched and s.sku_id:
                _sku_str = str(s.sku_id)
                if _sku_str in inv_by_sku:
                    _planned_matched.add(s.coupang_option_id)
                    _inv_item = inv_by_sku[_sku_str]
                    if _inv_item.barcode:
                        _inv_matched.add(_inv_item.barcode)
                    matched = True

        missing_in_invoice_skus = [s for s in planned_skus if s.coupang_option_id not in _planned_matched]
        extra_in_invoice = set(inv_by_barcode.keys()) - _inv_matched
        if missing_in_invoice_skus:
            _miss_items = []
            for _ps in missing_in_invoice_skus:
                _miss_items.append({
                    "coupang_barcode": _ps.coupang_barcode,
                    "wms_barcode": _ps.own_wms_barcode,
                    "coupang_option_id": _ps.coupang_option_id,
                    "상품명": _ps.product_name,
                    "수량": _ps.inbound_qty,
                })
            report.add(
                CheckItem(name="거래명세서 SKU 누락", status="warning",
                          detail=f"우리 발주에 있지만 거래명세서에 없음: {len(missing_in_invoice_skus)}건",
                          items=_miss_items)
            )
        if extra_in_invoice:
            report.add(
                CheckItem(name="거래명세서 SKU 추가", status="warning",
                          detail=f"거래명세서에 있지만 우리 발주에 없음: {len(extra_in_invoice)}건",
                          items=[{"barcode": bc, "수량": inv_by_barcode[bc].confirmed_qty} for bc in extra_in_invoice])
            )
        if not missing_in_invoice_skus and not extra_in_invoice:
            report.add(
                CheckItem(name="거래명세서 SKU 구성 일치", status="ok")
            )

        # 8e. 소비기한 일치
        exp_mismatches = []
        for bc, inv_item in inv_by_barcode.items():
            planned = planned_by_barcode.get(bc)
            if not planned or not planned.expected_expiry or not inv_item.expiry:
                continue
            if planned.expected_expiry != inv_item.expiry:
                exp_mismatches.append({
                    "barcode": bc,
                    "우리소비기한": planned.expected_expiry.isoformat(),
                    "거래명세서소비기한": inv_item.expiry.isoformat(),
                })
        if exp_mismatches:
            report.add(
                CheckItem(name="거래명세서 소비기한 일치", status="warning",
                          detail=f"{len(exp_mismatches)}건 불일치", items=exp_mismatches)
            )
        else:
            report.add(CheckItem(name="거래명세서 소비기한 일치", status="ok"))

        # 8f. 제조일자 일치
        mfg_mismatches = []
        for bc, inv_item in inv_by_barcode.items():
            planned = planned_by_barcode.get(bc)
            if not planned or not planned.expected_manufacture or not inv_item.manufacture:
                continue
            if planned.expected_manufacture != inv_item.manufacture:
                mfg_mismatches.append({
                    "barcode": bc,
                    "우리제조일자": planned.expected_manufacture.isoformat(),
                    "거래명세서제조일자": inv_item.manufacture.isoformat(),
                })
        if mfg_mismatches:
            report.add(
                CheckItem(name="거래명세서 제조일자 일치", status="warning",
                          detail=f"{len(mfg_mismatches)}건 불일치", items=mfg_mismatches)
            )
        else:
            report.add(CheckItem(name="거래명세서 제조일자 일치", status="ok"))

    elif invoice is None:
        report.add(CheckItem(name="거래명세서 검증", status="ok", detail="물류동봉문서 미제공 — 건너뜀"))

    # ----- 9. 메타 검증 (FC, 도착예정일) -----
    if attachment.fc_name:
        report.add(
            CheckItem(
                name="FC 정보",
                status="ok",
                actual=f"{attachment.fc_name}({attachment.fc_code})" if attachment.fc_code else attachment.fc_name,
            )
        )
    if attachment.arrival_date:
        report.add(
            CheckItem(
                name="도착예정일",
                status="ok",
                actual=attachment.arrival_date.isoformat(),
            )
        )
    if attachment.milkrun_id:
        report.add(
            CheckItem(name="밀크런ID", status="ok", actual=attachment.milkrun_id)
        )

    return report
