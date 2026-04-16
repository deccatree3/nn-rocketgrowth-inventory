"""SQLAlchemy ORM 모델."""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# 제품 마스터 (2-테이블 구조)
# - WmsProduct: WMS바코드 단위 (박스낱수, 중량, 소비기한일수 등 물리 속성)
# - CoupangProduct: 쿠팡 옵션ID 단위 (판매 속성 + 수동입고여부 + WMS바코드 매핑)
# ---------------------------------------------------------------------------
class WmsProduct(Base):
    __tablename__ = "wms_product"

    wms_barcode: Mapped[str] = mapped_column(String(64), primary_key=True)
    company_name: Mapped[str] = mapped_column(String(64), default="서현")
    product_name: Mapped[str | None] = mapped_column(Text)
    unit_qty: Mapped[int | None] = mapped_column(Integer)           # 낱개수량 (1, 2, 6…)
    parent_wms_barcode: Mapped[str | None] = mapped_column(String(64), index=True)
    box_qty: Mapped[int | None] = mapped_column(Integer)            # 1카톤박스 낱수
    weight_g: Mapped[int | None] = mapped_column(Integer)
    shelf_life_days: Mapped[int | None] = mapped_column(Integer)    # 유통기한 일수
    coupang_option_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    parent_coupang_option_id: Mapped[int | None] = mapped_column(BigInteger)
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class CoupangProduct(Base):
    __tablename__ = "coupang_product"

    coupang_option_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_name: Mapped[str] = mapped_column(String(64), default="서현")
    coupang_product_id: Mapped[int | None] = mapped_column(BigInteger)
    sku_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    product_name: Mapped[str] = mapped_column(Text, nullable=False)
    option_name: Mapped[str | None] = mapped_column(Text)
    grade: Mapped[str | None] = mapped_column(String(32))
    registered_at: Mapped[date | None] = mapped_column(Date)
    milkrun_managed: Mapped[bool] = mapped_column(Boolean, default=False)  # 수동입고여부
    wms_barcode: Mapped[str | None] = mapped_column(String(64), index=True)
    coupang_barcode: Mapped[str | None] = mapped_column(String(64))
    wms_barcode_return: Mapped[str | None] = mapped_column(String(64))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


# ---------------------------------------------------------------------------
# 쿠팡 재고 스냅샷
# ---------------------------------------------------------------------------
class CoupangInventorySnapshot(Base):
    __tablename__ = "coupang_inventory_snapshot"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(16), nullable=False)  # 'file' | 'api'
    source_file: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    items: Mapped[list["CoupangInventoryItem"]] = relationship(
        back_populates="snapshot", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("snapshot_date", "source_type", name="uq_coupang_snapshot_date_src"),
    )


class CoupangInventoryItem(Base):
    __tablename__ = "coupang_inventory_item"

    snapshot_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("coupang_inventory_snapshot.id", ondelete="CASCADE")
    )
    coupang_option_id: Mapped[int] = mapped_column(BigInteger)
    coupang_product_id: Mapped[int | None] = mapped_column(BigInteger)
    sku_id: Mapped[int | None] = mapped_column(BigInteger)
    product_name: Mapped[str | None] = mapped_column(Text)
    option_name: Mapped[str | None] = mapped_column(Text)
    sales_qty_7d: Mapped[int | None] = mapped_column(Integer)
    sales_qty_30d: Mapped[int | None] = mapped_column(Integer)
    orderable_stock: Mapped[int | None] = mapped_column(Integer)
    inbound_stock: Mapped[int | None] = mapped_column(Integer)
    storage_fee_month: Mapped[float | None] = mapped_column(Numeric(12, 2))
    expiry_1_30: Mapped[int | None] = mapped_column(Integer)
    expiry_31_45: Mapped[int | None] = mapped_column(Integer)
    expiry_46_60: Mapped[int | None] = mapped_column(Integer)
    expiry_61_120: Mapped[int | None] = mapped_column(Integer)
    expiry_121_180: Mapped[int | None] = mapped_column(Integer)
    expiry_181_plus: Mapped[int | None] = mapped_column(Integer)
    recommendation: Mapped[str | None] = mapped_column(Text)
    raw: Mapped[dict | None] = mapped_column(JSONB)

    snapshot: Mapped[CoupangInventorySnapshot] = relationship(back_populates="items")

    __table_args__ = (
        PrimaryKeyConstraint("snapshot_id", "coupang_option_id", name="pk_coupang_item"),
    )


# ---------------------------------------------------------------------------
# WMS 재고 스냅샷
# ---------------------------------------------------------------------------
class WmsInventorySnapshot(Base):
    __tablename__ = "wms_inventory_snapshot"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source_file: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    items: Mapped[list["WmsInventoryItem"]] = relationship(
        back_populates="snapshot", cascade="all, delete-orphan"
    )


class WmsInventoryItem(Base):
    __tablename__ = "wms_inventory_item"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("wms_inventory_snapshot.id", ondelete="CASCADE"), index=True
    )
    barcode: Mapped[str | None] = mapped_column(String(64), index=True)
    product_name: Mapped[str | None] = mapped_column(Text)
    loc_group: Mapped[str | None] = mapped_column(Text)      # 보관/출고대기 등
    loc: Mapped[str | None] = mapped_column(Text)
    total_qty: Mapped[int | None] = mapped_column(Integer)
    alloc_qty: Mapped[int | None] = mapped_column(Integer)
    available_qty: Mapped[int | None] = mapped_column(Integer)
    expiry_short: Mapped[date | None] = mapped_column(Date)  # 짧은 유통기한
    expiry_long: Mapped[date | None] = mapped_column(Date)   # 긴 유통기한
    raw: Mapped[dict | None] = mapped_column(JSONB)

    snapshot: Mapped[WmsInventorySnapshot] = relationship(back_populates="items")


# ---------------------------------------------------------------------------
# 입고 계획
# ---------------------------------------------------------------------------
class InboundPlan(Base):
    __tablename__ = "inbound_plan"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_name: Mapped[str] = mapped_column(String(64), default="서현")
    shipment_type: Mapped[str] = mapped_column(String(16), default="milkrun")  # milkrun|parcel
    plan_date: Mapped[date | None] = mapped_column(Date, index=True)
    fc_name: Mapped[str | None] = mapped_column(String(32))
    worker: Mapped[str | None] = mapped_column(String(64))
    coupang_snapshot_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("coupang_inventory_snapshot.id")
    )
    wms_snapshot_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("wms_inventory_snapshot.id")
    )
    status: Mapped[str] = mapped_column(String(16), default="draft")  # draft|verified|completed
    note: Mapped[str | None] = mapped_column(Text)
    # 검수 단계에서 채워지는 값
    milkrun_id: Mapped[str | None] = mapped_column(String(32))
    arrival_date: Mapped[date | None] = mapped_column(Date)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    total_pallets: Mapped[int | None] = mapped_column(Integer)
    total_weight_kg: Mapped[float | None] = mapped_column(Numeric(10, 2))
    # 재고이동건 템플릿 파일 (검수 단계에서 시트 추가용)
    movement_template_blob: Mapped[bytes | None] = mapped_column(LargeBinary)
    movement_template_filename: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    items: Mapped[list["InboundPlanItem"]] = relationship(
        back_populates="plan", cascade="all, delete-orphan"
    )


class InboundPlanItem(Base):
    __tablename__ = "inbound_plan_item"

    plan_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("inbound_plan.id", ondelete="CASCADE")
    )
    coupang_option_id: Mapped[int] = mapped_column(BigInteger)
    product_name: Mapped[str | None] = mapped_column(Text)
    option_name: Mapped[str | None] = mapped_column(Text)
    current_total_stock: Mapped[int | None] = mapped_column(Integer)
    sales_7d: Mapped[int | None] = mapped_column(Integer)
    sales_30d: Mapped[int | None] = mapped_column(Integer)
    sales_velocity_daily: Mapped[float | None] = mapped_column(Numeric(12, 4))
    stock_after_1w: Mapped[float | None] = mapped_column(Numeric(12, 2))
    stock_after_2w: Mapped[float | None] = mapped_column(Numeric(12, 2))
    stock_after_4w: Mapped[float | None] = mapped_column(Numeric(12, 2))
    box_qty: Mapped[int | None] = mapped_column(Integer)
    inbound_qty_suggested: Mapped[int | None] = mapped_column(Integer)
    inbound_qty_final: Mapped[int | None] = mapped_column(Integer)
    inbound_boxes: Mapped[int | None] = mapped_column(Integer)
    days_sellable_after: Mapped[float | None] = mapped_column(Numeric(12, 2))
    wms_short_expiry: Mapped[date | None] = mapped_column(Date)
    wms_long_expiry: Mapped[date | None] = mapped_column(Date)
    # 검수 단계에서 채워지는 필드
    pallet_no: Mapped[int | None] = mapped_column(Integer)
    barcode_attached: Mapped[str | None] = mapped_column(String(64))
    barcode_type: Mapped[str | None] = mapped_column(String(16))  # 88코드|쿠팡바코드
    note: Mapped[str | None] = mapped_column(Text)

    plan: Mapped[InboundPlan] = relationship(back_populates="items")

    __table_args__ = (
        PrimaryKeyConstraint("plan_id", "coupang_option_id", name="pk_inbound_plan_item"),
    )


# ---------------------------------------------------------------------------
# 감사 로그
# ---------------------------------------------------------------------------
class CoupangResultLog(Base):
    """쿠팡 어드민 결과물(라벨/물류부착문서) 이력. 중복/과거 감지용."""

    __tablename__ = "coupang_result_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_name: Mapped[str] = mapped_column(String(64), default="서현")
    milkrun_id: Mapped[str] = mapped_column(String(32), nullable=False)
    fc_name: Mapped[str] = mapped_column(String(32), nullable=False)
    arrival_date: Mapped[date] = mapped_column(Date, nullable=False)
    total_pallets: Mapped[int | None] = mapped_column(Integer)
    total_boxes: Mapped[int | None] = mapped_column(Integer)
    total_skus: Mapped[int | None] = mapped_column(Integer)
    plan_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("inbound_plan.id"))
    label_filename: Mapped[str | None] = mapped_column(Text)
    attachment_filename: Mapped[str | None] = mapped_column(Text)
    verified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    verifier: Mapped[str | None] = mapped_column(String(64))
    raw_meta: Mapped[dict | None] = mapped_column(JSONB)

    __table_args__ = (
        UniqueConstraint("company_name", "milkrun_id", name="uq_coupang_result_log_company_milkrun"),
    )


class ActivityLog(Base):
    __tablename__ = "activity_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    actor: Mapped[str | None] = mapped_column(String(64))
    action: Mapped[str] = mapped_column(String(64))
    entity: Mapped[str | None] = mapped_column(String(64))
    entity_id: Mapped[str | None] = mapped_column(String(64))
    detail: Mapped[dict | None] = mapped_column(JSONB)
