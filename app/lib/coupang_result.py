"""쿠팡 어드민 결과물(PDF) 파서.

두 가지 파일을 처리:
  1) sku-barcode-labels-*.pdf  → SKU별 부착바코드/소비기한/라벨개수/상품명 추출
  2) 물류부착문서_*.pdf         → 팔레트 메타(밀크런ID, FC, 도착예정일, 팔레트수) 추출

라벨 PDF 구조:
  - 한 페이지 = 4 컬럼 × N 행 (보통 10행=40라벨)
  - 한 라벨 = 3줄(바코드+소비기한 / 상품명 줄1 / 상품명 줄2)
  - 같은 SKU 가 연속으로 나오며, SKU 경계에서 한 행 안에 두 SKU 가 섞일 수 있음

물류부착문서 PDF 구조:
  - 한 페이지 = 한 팔레트 부착문서
  - 페이지 수 = 총 팔레트 수
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import Any

import pdfplumber


# ============================================================================
# 라벨 PDF
# ============================================================================
@dataclass
class LabelInfo:
    barcode: str            # 부착바코드 (S00... 또는 88코드)
    expiry: date | None     # 소비기한
    count: int              # 이 SKU 의 라벨 총 개수
    raw_name: str | None    # 라벨에서 추출한 상품명 (정규화 전)


# 라벨 1개 패턴: "S00..." 또는 "880..." 시작 + 공백 + "소비기한" + YY.MM.DD
_LABEL_PATTERN = re.compile(
    r"((?:S\d{13}|8\d{12,13}))\s*소비기한\s*(\d{2})\.(\d{2})\.(\d{2})"
)


def _parse_expiry(yy: str, mm: str, dd: str) -> date | None:
    try:
        return date(2000 + int(yy), int(mm), int(dd))
    except (ValueError, TypeError):
        return None


def parse_barcode_labels(pdf_input: str | Path | bytes | BytesIO) -> dict[str, LabelInfo]:
    """라벨 PDF 파싱.

    Returns:
        {barcode: LabelInfo}  — 같은 바코드의 라벨을 합산
    """
    counts: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "expiry": None, "raw_name": None, "first_pos": None}
    )

    if isinstance(pdf_input, (bytes, bytearray)):
        pdf_input = BytesIO(pdf_input)

    with pdfplumber.open(pdf_input) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for m in _LABEL_PATTERN.finditer(text):
                barcode = m.group(1)
                expiry = _parse_expiry(m.group(2), m.group(3), m.group(4))
                slot = counts[barcode]
                slot["count"] += 1
                if slot["expiry"] is None:
                    slot["expiry"] = expiry
                # 첫 등장 위치 — 상품명 추출용
                if slot["first_pos"] is None:
                    slot["first_pos"] = (page.page_number, m.start(), text)

    # 상품명 추출 (best-effort): 첫 등장 위치의 다음 줄에서 첫 컬럼만 추출
    for barcode, slot in counts.items():
        if slot["first_pos"] is None:
            continue
        _, start_idx, page_text = slot["first_pos"]
        # 매치 라인의 끝 → 다음 라인 시작
        line_end = page_text.find("\n", start_idx)
        if line_end < 0:
            continue
        # 다음 1~2 줄 = 상품명 (여러 컬럼이 공백으로 나뉨)
        rest = page_text[line_end + 1:]
        next_lines = rest.split("\n", 2)[:2]  # 최대 2줄
        if not next_lines:
            continue
        # 가장 단순한 추출: 첫 줄에서 / 까지 (옵션 구분자)
        first_line = next_lines[0]
        # 첫 컬럼만 추출 — / 구분자로 분리되었을 가능성
        # "퍼펙토 프리미엄 독일 맥주효모환 / 퍼펙토 프리미엄 독일 맥주효모환 / ..."
        parts = first_line.split(" / ")
        if parts:
            name_main = parts[0].strip()
            # 두 번째 줄(옵션 부분)도 첫 컬럼만
            opt = ""
            if len(next_lines) >= 2:
                opt_parts = next_lines[1].split(" ")
                # 첫 컬럼 = 옵션 정보 (나머지는 반복)
                # 정확히 어디서 끝나는지 알기 어려우므로 그냥 첫 절반
                opt = " ".join(opt_parts[: max(1, len(opt_parts) // 4)])
            slot["raw_name"] = (name_main + " " + opt).strip()

    return {
        bc: LabelInfo(
            barcode=bc,
            expiry=info["expiry"],
            count=info["count"],
            raw_name=info["raw_name"],
        )
        for bc, info in counts.items()
    }


# ============================================================================
# 물류부착문서 PDF
# ============================================================================
@dataclass
class AttachmentMeta:
    milkrun_id: str | None = None
    fc_name: str | None = None
    fc_code: str | None = None        # 동탄1(17) 의 17 같은 값
    arrival_date: date | None = None
    company_name: str | None = None   # 주식회사 서현커머스
    box_barcode: str | None = None    # MRN9946685
    total_pallets: int = 0            # 페이지 수
    pallets: list[dict[str, Any]] = field(default_factory=list)  # [{no, label, page}]


_FC_LINE = re.compile(r"([가-힣\d]+)\(?(\d+)?\)?\s*\[로켓그로스\]\s*팔레트\s*(\S+)")
_MILKRUN_LINE = re.compile(r"^(\d{6,})\s+(\d{4}-\d{2}-\d{2})")
_BOX_BARCODE = re.compile(r"^(MRN\d+)")
_COMPANY_LINE = re.compile(r"(주식회사\s*[가-힣\w]+|㈜\s*[가-힣\w]+)")


def parse_attachment_doc(pdf_input: str | Path | bytes | BytesIO) -> AttachmentMeta:
    """물류부착문서 PDF 파싱.

    구조:
      - 팔레트 부착용 페이지: "팔레트 X-N" 형식, X=총팔레트수, N=순번
      - 같은 라벨이 두 번 (앞면/뒷면) 반복 출력될 수 있음
      - 마지막 페이지는 적재리스트 빈 양식 (메타 없음)

    총 팔레트수 = "X-N" 라벨에서 X 값 (가장 많이 등장한 X 사용)
    """
    meta = AttachmentMeta()

    if isinstance(pdf_input, (bytes, bytearray)):
        pdf_input = BytesIO(pdf_input)

    pallet_x_values: list[int] = []
    seen_labels: set[str] = set()

    with pdfplumber.open(pdf_input) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            lines = [l.strip() for l in text.split("\n") if l.strip()]

            pallet_label = None
            for line in lines:
                # 동탄1(17) [로켓그로스] 팔레트 4-1
                m = _FC_LINE.search(line)
                if m:
                    if not meta.fc_name:
                        meta.fc_name = m.group(1)
                        if m.group(2):
                            meta.fc_code = m.group(2)
                    pallet_label = m.group(3)  # "4-1"
                    # X-N 분리
                    parts = pallet_label.split("-")
                    if len(parts) == 2 and parts[0].isdigit():
                        pallet_x_values.append(int(parts[0]))
                    continue
                # 9946685 2026-04-14
                m = _MILKRUN_LINE.match(line)
                if m:
                    if not meta.milkrun_id:
                        meta.milkrun_id = m.group(1)
                    if not meta.arrival_date:
                        try:
                            y, mo, d = m.group(2).split("-")
                            meta.arrival_date = date(int(y), int(mo), int(d))
                        except (ValueError, TypeError):
                            pass
                    continue
                # MRN9946685
                m = _BOX_BARCODE.match(line)
                if m and not meta.box_barcode:
                    meta.box_barcode = m.group(1)
                    continue
                # 주식회사 서현커머스
                m = _COMPANY_LINE.search(line)
                if m and not meta.company_name:
                    meta.company_name = m.group(1).strip()

            if pallet_label and pallet_label not in seen_labels:
                seen_labels.add(pallet_label)
                meta.pallets.append({"page": page_idx, "label": pallet_label})

    # 총 팔레트수: "X-N" 의 X 가장 많이 나온 값 (= 총 팔레트수)
    if pallet_x_values:
        from collections import Counter
        meta.total_pallets = Counter(pallet_x_values).most_common(1)[0][0]
    else:
        meta.total_pallets = 0

    return meta


# ============================================================================
# 물류동봉문서(거래명세서) PDF
# ============================================================================
@dataclass
class InvoiceItem:
    no: int
    sku_id: str              # 상품번호
    barcode: str             # 상품 바코드
    order_qty: int           # 발주수량
    confirmed_qty: int       # 확정수량
    expiry: date | None      # 소비기한
    manufacture: date | None # 제조일자
    product_name: str | None # 상품명/옵션 (줄바꿈 포함 가능)


@dataclass
class InvoiceMeta:
    company_name: str | None = None
    company_code: str | None = None  # A00371983
    order_id: str | None = None      # 발주번호 = 128907348
    transport_type: str | None = None  # 밀크런
    arrival_date: date | None = None
    fc_name: str | None = None       # 동탄1(DON1)
    pallet_count: int | None = None
    total_order_qty: int | None = None
    total_confirmed_qty: int | None = None
    items: list[InvoiceItem] = field(default_factory=list)


_DATE_8 = re.compile(r"(\d{4})(\d{2})(\d{2})")


def _parse_date8(s: str) -> date | None:
    """YYYYMMDD → date."""
    m = _DATE_8.match(s.strip())
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    return None


def parse_invoice_doc(pdf_input: str | Path | bytes | BytesIO) -> InvoiceMeta:
    """물류동봉문서(거래명세서) PDF 파싱.

    구조:
      - Page 1~2: 쿠팡 제출용 (데이터)
      - Page 3~4: 업체 보관용 (동일 내용 반복, 무시)
    """
    meta = InvoiceMeta()

    if isinstance(pdf_input, (bytes, bytearray)):
        pdf_input = BytesIO(pdf_input)

    with pdfplumber.open(pdf_input) as pdf:
        # 첫 절반 페이지만 사용 (쿠팡 제출용)
        half = max(1, len(pdf.pages) // 2)
        full_text = ""
        for p in pdf.pages[:half]:
            full_text += (p.extract_text() or "") + "\n"

    lines = full_text.split("\n")

    # 메타 정보 추출
    for i, line in enumerate(lines):
        if "업체명" in line and not meta.company_name:
            parts = line.split("업체명")
            if len(parts) > 1:
                meta.company_name = parts[1].strip()
        elif "업체번호" in line and not meta.company_code:
            parts = line.split("업체번호")
            if len(parts) > 1:
                meta.company_code = parts[1].strip()
        elif "발주번호" in line and not meta.order_id:
            m = re.search(r"발주번호\s*(\d+)", line)
            if m:
                meta.order_id = m.group(1)
        elif "운송타입" in line and not meta.transport_type:
            parts = line.split("운송타입")
            if len(parts) > 1:
                meta.transport_type = parts[1].strip()
        elif "도착예정일" in line and not meta.arrival_date:
            m = re.search(r"(\d{8})", line)
            if m:
                meta.arrival_date = _parse_date8(m.group(1))
        elif "납품 센터 " in line and not meta.fc_name:
            parts = line.split("납품 센터")
            if len(parts) > 1:
                meta.fc_name = parts[1].strip().split("(")[0].strip()
        elif "팔레트수량" in line and meta.pallet_count is None:
            m = re.search(r"팔레트수량\s*(\d+)", line)
            if m:
                meta.pallet_count = int(m.group(1))
        elif line.strip().startswith("합계"):
            nums = re.findall(r"\d+", line)
            if len(nums) >= 2:
                meta.total_order_qty = int(nums[0])
                meta.total_confirmed_qty = int(nums[1])

    # 상품 정보 추출: "No 상품번호 [옵션텍스트] 발주수량 확정수량" 패턴
    # 옵션 텍스트가 줄바꿈되어 상품번호 줄에 합쳐지는 경우 대응
    # 예: "4 16972922 2개입 32 32" 또는 "5 20238360 30포(60g) 60 60"
    item_line_pat = re.compile(r"^(\d+)\s+(\d{5,})\s+.*?(\d+)\s+(\d+)\s*$")
    barcode_line_pat = re.compile(r"^((?:S\d{13}|8\d{12,13}))(?:\s|$)")
    expiry_pat = re.compile(r"Y\s+(\d{8})")

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m = item_line_pat.match(line)
        if m:
            no = int(m.group(1))
            sku_id = m.group(2)
            order_qty = int(m.group(3))
            confirmed_qty = int(m.group(4))

            # 상품명: 이전 줄들에서 추출 (No 줄 위에 있음)
            name_parts = []
            for back in range(1, 4):
                if i - back >= 0:
                    prev = lines[i - back].strip()
                    if prev and not prev.startswith("No ") and not prev.startswith("확정수량") and not prev.startswith("Box"):
                        name_parts.insert(0, prev)
                    else:
                        break
            product_name = " ".join(name_parts).strip() if name_parts else None
            # 이름에서 제조일자 부분 제거 (Y YYYYMMDD)
            if product_name:
                product_name = re.sub(r"\s*[YN]\s*\d{8}\s*$", "", product_name).strip()
                product_name = re.sub(r"\s*[YN]\s*-\s*$", "", product_name).strip()

            # 다음 줄들에서 바코드 + 소비기한 탐색 (1~3줄 뒤까지)
            barcode = ""
            expiry = None
            manufacture = None
            for ahead in range(1, 4):
                if i + ahead >= len(lines):
                    break
                next_line = lines[i + ahead].strip()
                if not barcode:
                    bm = barcode_line_pat.match(next_line)
                    if bm:
                        barcode = bm.group(1)
                if not expiry:
                    em = expiry_pat.search(next_line)
                    if em:
                        expiry = _parse_date8(em.group(1))
                if barcode and expiry:
                    break

            # 제조일자: No 줄 바로 위의 Y YYYYMMDD
            for back in range(1, 4):
                if i - back >= 0:
                    prev = lines[i - back].strip()
                    em2 = re.search(r"Y\s+(\d{8})", prev)
                    if em2:
                        manufacture = _parse_date8(em2.group(1))
                        break

            meta.items.append(
                InvoiceItem(
                    no=no,
                    sku_id=sku_id,
                    barcode=barcode,
                    order_qty=order_qty,
                    confirmed_qty=confirmed_qty,
                    expiry=expiry,
                    manufacture=manufacture,
                    product_name=product_name,
                )
            )
        i += 1

    return meta


# ============================================================================
# 라벨 검증 도우미: 상품명 정규화
# ============================================================================
def normalize_product_name(name: str | None) -> str:
    """상품명 비교를 위한 정규화.

    제거: 공백, 쉼표, 슬래시, 괄호, 한자, 단위 표기('포'↔'p', '개입' 등 변형)
    """
    if not name:
        return ""
    s = name
    # 단위 정규화
    s = re.sub(r"\b(\d+)\s*포\b", r"\1p", s)
    s = re.sub(r"\b(\d+)\s*개입\b", r"\1개", s)
    # 특수문자 제거
    s = re.sub(r"[\s,/()\[\]{}.\-+]", "", s)
    return s.lower()


def name_similarity(a: str | None, b: str | None) -> float:
    """0.0 ~ 1.0 유사도. 단순 토큰 교집합/합집합."""
    na, nb = normalize_product_name(a), normalize_product_name(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    # 한 쪽이 다른 쪽 포함
    if na in nb or nb in na:
        return 0.85
    # 토큰 (3-gram) 교집합
    def grams(s: str, n: int = 3) -> set[str]:
        return {s[i : i + n] for i in range(len(s) - n + 1)} if len(s) >= n else {s}

    ga, gb = grams(na), grams(nb)
    if not ga or not gb:
        return 0.0
    return len(ga & gb) / len(ga | gb)
