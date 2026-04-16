"""company name + milkrun metadata + coupang_result_log

Revision ID: 0003
Revises: 0002
Create Date: 2026-04-12

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DEFAULT_COMPANY = "서현"


def upgrade() -> None:
    # ----- inbound_plan: 업체/모드/검수 메타 컬럼 추가 -----
    op.add_column(
        "inbound_plan",
        sa.Column(
            "company_name",
            sa.String(length=64),
            server_default=DEFAULT_COMPANY,
            nullable=False,
        ),
    )
    op.add_column(
        "inbound_plan",
        sa.Column(
            "shipment_type",
            sa.String(length=16),
            server_default="milkrun",
            nullable=False,
        ),
    )
    op.add_column("inbound_plan", sa.Column("milkrun_id", sa.String(length=32), nullable=True))
    op.add_column("inbound_plan", sa.Column("arrival_date", sa.Date(), nullable=True))
    op.add_column(
        "inbound_plan",
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "inbound_plan",
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column("inbound_plan", sa.Column("total_pallets", sa.Integer(), nullable=True))
    op.add_column(
        "inbound_plan",
        sa.Column("total_weight_kg", sa.Numeric(10, 2), nullable=True),
    )

    # ----- inbound_plan_item: 검수 결과 채움 컬럼 -----
    op.add_column("inbound_plan_item", sa.Column("pallet_no", sa.Integer(), nullable=True))
    op.add_column(
        "inbound_plan_item",
        sa.Column("barcode_attached", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "inbound_plan_item",
        sa.Column("barcode_type", sa.String(length=16), nullable=True),
    )

    # ----- 마스터 테이블에도 업체명 -----
    op.add_column(
        "wms_product",
        sa.Column(
            "company_name",
            sa.String(length=64),
            server_default=DEFAULT_COMPANY,
            nullable=False,
        ),
    )
    op.add_column(
        "coupang_product",
        sa.Column(
            "company_name",
            sa.String(length=64),
            server_default=DEFAULT_COMPANY,
            nullable=False,
        ),
    )

    # ----- coupang_result_log: 신규 -----
    op.create_table(
        "coupang_result_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "company_name",
            sa.String(length=64),
            server_default=DEFAULT_COMPANY,
            nullable=False,
        ),
        sa.Column("milkrun_id", sa.String(length=32), nullable=False),
        sa.Column("fc_name", sa.String(length=32), nullable=False),
        sa.Column("arrival_date", sa.Date(), nullable=False),
        sa.Column("total_pallets", sa.Integer()),
        sa.Column("total_boxes", sa.Integer()),
        sa.Column("total_skus", sa.Integer()),
        sa.Column(
            "plan_id",
            sa.BigInteger(),
            sa.ForeignKey("inbound_plan.id"),
            nullable=True,
        ),
        sa.Column("label_filename", sa.Text()),
        sa.Column("attachment_filename", sa.Text()),
        sa.Column(
            "verified_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("verifier", sa.String(length=64)),
        sa.Column("raw_meta", postgresql.JSONB()),
        sa.UniqueConstraint("company_name", "milkrun_id", name="uq_coupang_result_log_company_milkrun"),
    )


def downgrade() -> None:
    op.drop_table("coupang_result_log")
    op.drop_column("coupang_product", "company_name")
    op.drop_column("wms_product", "company_name")
    op.drop_column("inbound_plan_item", "barcode_type")
    op.drop_column("inbound_plan_item", "barcode_attached")
    op.drop_column("inbound_plan_item", "pallet_no")
    op.drop_column("inbound_plan", "total_weight_kg")
    op.drop_column("inbound_plan", "total_pallets")
    op.drop_column("inbound_plan", "verified_at")
    op.drop_column("inbound_plan", "submitted_at")
    op.drop_column("inbound_plan", "arrival_date")
    op.drop_column("inbound_plan", "milkrun_id")
    op.drop_column("inbound_plan", "shipment_type")
    op.drop_column("inbound_plan", "company_name")
