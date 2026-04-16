"""split product_master into wms_product + coupang_product

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-05

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 기존 product_master 제거 (MVP 단계, 데이터 유지 안 함)
    op.drop_table("product_master")

    # --- wms_product ---------------------------------------------------
    op.create_table(
        "wms_product",
        sa.Column("wms_barcode", sa.String(length=64), primary_key=True),
        sa.Column("product_name", sa.Text()),
        sa.Column("unit_qty", sa.Integer()),  # 낱개수량 (1, 2, 6 ...)
        sa.Column("parent_wms_barcode", sa.String(length=64), index=True),
        sa.Column("box_qty", sa.Integer()),
        sa.Column("weight_g", sa.Integer()),
        sa.Column("shelf_life_days", sa.Integer()),
        sa.Column("coupang_option_id", sa.BigInteger(), index=True),
        sa.Column("parent_coupang_option_id", sa.BigInteger()),
        sa.Column("note", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # --- coupang_product -----------------------------------------------
    op.create_table(
        "coupang_product",
        sa.Column("coupang_option_id", sa.BigInteger(), primary_key=True),
        sa.Column("coupang_product_id", sa.BigInteger()),
        sa.Column("sku_id", sa.BigInteger(), index=True),
        sa.Column("product_name", sa.Text(), nullable=False),
        sa.Column("option_name", sa.Text()),
        sa.Column("grade", sa.String(length=32)),
        sa.Column("registered_at", sa.Date()),
        sa.Column(
            "milkrun_managed",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("wms_barcode", sa.String(length=64), index=True),
        sa.Column("coupang_barcode", sa.String(length=64)),
        sa.Column("wms_barcode_return", sa.String(length=64)),
        sa.Column(
            "active",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column("note", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("coupang_product")
    op.drop_table("wms_product")
    # product_master 재생성은 0001로 회귀하면 됨 (downgrade chain)
    op.create_table(
        "product_master",
        sa.Column("coupang_option_id", sa.BigInteger(), primary_key=True),
        sa.Column("coupang_product_id", sa.BigInteger()),
        sa.Column("sku_id", sa.BigInteger(), index=True),
        sa.Column("product_name", sa.Text(), nullable=False),
        sa.Column("option_name", sa.Text()),
        sa.Column("wms_barcode", sa.String(length=64), index=True),
        sa.Column("wms_barcode_alt", sa.String(length=64)),
        sa.Column("box_qty", sa.Integer()),
        sa.Column("shelf_life_days", sa.Integer()),
        sa.Column("weight_g", sa.Integer()),
        sa.Column("category", sa.Text()),
        sa.Column("short_pkg", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
