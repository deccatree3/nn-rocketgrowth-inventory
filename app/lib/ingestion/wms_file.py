"""물류창고(WMS) 재고현황 파일(Document_*.xls) 파서.

파일 구조 (실측):
- 시트: 첫 시트
- 1행: 헤더
- 2행~: 데이터. 한 바코드가 여러 LOC/LOT 단위로 분할되어 여러 행에 존재.
        같은 바코드라도 로트(=생산 배치)가 다르면 유통일이 다를 수 있다.

컬럼 매핑 (0-indexed):
  0  품목코드 (WMS 바코드)
  1  품목명
  2  품목손상플래그
  3  LOC그룹            (메인보관/출고대기/피킹존 등)
  4  OWNERLOCGROUP
  5  LOC
  6  재고수량 (total)
  7  할당수량 (alloc)
 11  가능수량 (available)
 12  유통기간          (보통 비어있음)
 14  속성4(제조일)      (보통 비어있음, 파일 변형)
 17  속성5(유통일)      (Excel serial date, 배치별 유통기한)

동일 바코드의 여러 행은 LOC/LOT 단위 분할이며, 유통일이 다른 경우
각각이 **독립된 배치(batch)** 로 취급되어야 한다 (출고 시 혼적 금지).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import xlrd

from .base import WmsInventoryRow, WmsSnapshot


def _to_int(v: Any) -> int | None:
    if v is None or v == "" or v == "-":
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _to_str_opt(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _excel_serial_to_date(v: Any, book_datemode: int) -> date | None:
    """엑셀 serial 날짜 → date. 문자열로 온 경우도 대응."""
    if v is None or v == "" or v == "-":
        return None
    if isinstance(v, (int, float)):
        if v <= 0:
            return None
        try:
            y, m, d, _, _, _ = xlrd.xldate_as_tuple(v, book_datemode)
            return date(y, m, d)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


_DATE_IN_NAME = re.compile(r"Document_(\d{4})-(\d{2})-(\d{2})")


def _infer_snapshot_date(filename: str) -> date:
    m = _DATE_IN_NAME.search(filename)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return date.today()


_HEADER_ALIASES = {
    "barcode": ["품목코드"],
    "product_name": ["품목명"],
    "loc_group": ["LOC그룹"],
    "loc": ["LOC"],
    "total_qty": ["재고수량"],
    "alloc_qty": ["할당수량"],
    "available_qty": ["가능수량"],
    "expiry": ["속성5(유통일)", "속성5", "유통일"],
}


def _resolve_headers(header_row: list) -> dict[str, int]:
    """헤더 row 를 읽어서 필드 → 컬럼 인덱스 매핑 반환 (매핑 실패 시 폴백용 기본값 사용)."""
    lookup: dict[str, int] = {}
    for idx, cell in enumerate(header_row):
        name = str(cell).strip() if cell is not None else ""
        if not name:
            continue
        for field, aliases in _HEADER_ALIASES.items():
            if name in aliases and field not in lookup:
                lookup[field] = idx
    return lookup


def parse_wms_inventory_file(path: str | Path) -> WmsSnapshot:
    """WMS Document_*.xls 를 파싱하여 WmsSnapshot 반환.

    각 raw row = 1 LOC/LOT. expiry_short 에 해당 배치의 유통일(속성5)을 넣는다.
    expiry_long 은 사용하지 않는다(배치 구분은 expiry_short 기준).

    컬럼 인덱스는 헤더명으로 동적 매핑한다 (업체·쿼리별 컬럼 순서 차이 대응).
    """
    path = Path(path)
    wb = xlrd.open_workbook(str(path))
    ws = wb.sheet_by_index(0)
    datemode = wb.datemode

    header = ws.row_values(0) if ws.nrows > 0 else []
    cols = _resolve_headers(header)

    def _get(row: list, field: str, fallback_idx: int):
        idx = cols.get(field, fallback_idx)
        return row[idx] if 0 <= idx < len(row) else None

    rows: list[WmsInventoryRow] = []
    for i in range(1, ws.nrows):
        r = ws.row_values(i)
        barcode = _to_str_opt(_get(r, "barcode", 0))
        if not barcode:
            continue

        row = WmsInventoryRow(
            barcode=barcode,
            product_name=_to_str_opt(_get(r, "product_name", 1)),
            loc_group=_to_str_opt(_get(r, "loc_group", 3)),
            loc=_to_str_opt(_get(r, "loc", 5)),
            total_qty=_to_int(_get(r, "total_qty", 6)),
            alloc_qty=_to_int(_get(r, "alloc_qty", 7)),
            available_qty=_to_int(_get(r, "available_qty", 11)),
            expiry_short=_excel_serial_to_date(_get(r, "expiry", 17), datemode),
            expiry_long=None,  # 단일 배치 유통일만 사용
            raw={str(j): (str(v) if v not in (None, "") else None) for j, v in enumerate(r)},
        )
        rows.append(row)

    return WmsSnapshot(
        snapshot_date=_infer_snapshot_date(path.name),
        source_file=path.name,
        rows=rows,
    )


#: LOC 이 이 값인 행은 가능재고에서 제외 (이미 출고 대기/피킹 완료 상태)
EXCLUDED_LOCS = {"RELEASEAREA"}


def aggregate_wms_by_barcode(
    snapshot: WmsSnapshot,
    excluded_locs: set[str] = EXCLUDED_LOCS,
) -> dict[str, dict[str, Any]]:
    """바코드별 요약 + **유통일 기준 배치 리스트** 반환.

    LOC ∈ `excluded_locs` (기본: RELEASEAREA) 인 행은 **가능재고에 포함되지 않는다**.
    총재고(total_qty) / 배치 total 에도 반영하지 않는다 (이미 출고 절차에 들어간 재고).

    배치(batch) = 동일 유통일을 공유하는 행들의 가용수량 합계.
    LOC 그룹은 무시하고 expiry_date 만으로 그룹화한다.

    Returns:
        {
            barcode: {
                "total_qty": int,                 # 전체 재고수량
                "available_qty": int,             # 전체 가용수량
                "alloc_qty": int,                 # 전체 할당수량
                "product_name": str|None,
                "batches": [                      # expiry_date 오름차순
                    {"expiry": date, "available": int, "total": int},
                    ...
                ],
                "expiry_short": date|None,        # 가장 빠른 유통일 (호환용)
                "expiry_long": date|None,         # 가장 늦은 유통일 (호환용)
            }
        }
    """
    # (barcode, expiry_date) 단위 집계
    batch_map: dict[tuple[str, Any], dict[str, int]] = {}
    total_map: dict[str, dict[str, Any]] = {}

    excluded_norm = {s.strip().upper() for s in excluded_locs}

    for row in snapshot.rows:
        if not row.barcode:
            continue
        # 제외 LOC 필터 (RELEASEAREA 등)
        if row.loc and row.loc.strip().upper() in excluded_norm:
            continue

        t = total_map.setdefault(
            row.barcode,
            {
                "total_qty": 0,
                "available_qty": 0,
                "alloc_qty": 0,
                "product_name": row.product_name,
            },
        )
        t["total_qty"] += row.total_qty or 0
        t["available_qty"] += row.available_qty or 0
        t["alloc_qty"] += row.alloc_qty or 0

        # 배치 키: (barcode, expiry). expiry가 없으면 None → '미표시' 배치
        key = (row.barcode, row.expiry_short)
        b = batch_map.setdefault(key, {"available": 0, "total": 0})
        b["available"] += row.available_qty or 0
        b["total"] += row.total_qty or 0

    # barcode → batches 리스트
    agg: dict[str, dict[str, Any]] = {}
    for (barcode, expiry), qtys in batch_map.items():
        a = agg.setdefault(
            barcode,
            {
                **total_map[barcode],
                "batches": [],
                "expiry_short": None,
                "expiry_long": None,
            },
        )
        a["batches"].append({"expiry": expiry, "available": qtys["available"], "total": qtys["total"]})

    for barcode, a in agg.items():
        # 유통일 오름차순 (None 은 맨 뒤)
        a["batches"].sort(key=lambda b: (b["expiry"] is None, b["expiry"]))
        dated = [b for b in a["batches"] if b["expiry"] is not None]
        if dated:
            a["expiry_short"] = dated[0]["expiry"]
            a["expiry_long"] = dated[-1]["expiry"]
    return agg
