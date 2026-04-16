"""데이터 수집 레이어 공통 타입.

파일 업로드와 향후 쿠팡 API 클라이언트가 동일한 dataclass를 반환하도록 추상화한다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Protocol


@dataclass
class CoupangInventoryRow:
    coupang_option_id: int
    coupang_product_id: int | None
    sku_id: int | None
    product_name: str | None
    option_name: str | None
    sales_qty_7d: int
    sales_qty_30d: int
    orderable_stock: int
    inbound_stock: int
    storage_fee_month: float | None
    expiry_1_30: int
    expiry_31_45: int
    expiry_46_60: int
    expiry_61_120: int
    expiry_121_180: int
    expiry_181_plus: int
    recommendation: str | None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class CoupangSnapshot:
    snapshot_date: date
    source_type: str  # 'file' | 'api'
    source_file: str | None
    rows: list[CoupangInventoryRow]


@dataclass
class WmsInventoryRow:
    barcode: str | None
    product_name: str | None
    loc_group: str | None
    loc: str | None
    total_qty: int | None
    alloc_qty: int | None
    available_qty: int | None
    expiry_short: date | None
    expiry_long: date | None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class WmsSnapshot:
    snapshot_date: date
    source_file: str | None
    rows: list[WmsInventoryRow]


class CoupangSource(Protocol):
    def fetch(self) -> CoupangSnapshot: ...


class WmsSource(Protocol):
    def fetch(self) -> WmsSnapshot: ...
