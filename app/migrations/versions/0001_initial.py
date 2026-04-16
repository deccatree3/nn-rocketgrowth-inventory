"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-04-05

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- product_master -----------------------------------------------------
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

    # --- coupang snapshot ---------------------------------------------------
    op.create_table(
        "coupang_inventory_snapshot",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("snapshot_date", sa.Date(), nullable=False, index=True),
        sa.Column("source_type", sa.String(length=16), nullable=False),
        sa.Column("source_file", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("snapshot_date", "source_type", name="uq_coupang_snapshot_date_src"),
    )

    op.create_table(
        "coupang_inventory_item",
        sa.Column(
            "snapshot_id",
            sa.BigInteger(),
            sa.ForeignKey("coupang_inventory_snapshot.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("coupang_option_id", sa.BigInteger(), nullable=False),
        sa.Column("coupang_product_id", sa.BigInteger()),
        sa.Column("sku_id", sa.BigInteger()),
        sa.Column("product_name", sa.Text()),
        sa.Column("option_name", sa.Text()),
        sa.Column("sales_qty_7d", sa.Integer()),
        sa.Column("sales_qty_30d", sa.Integer()),
        sa.Column("orderable_stock", sa.Integer()),
        sa.Column("inbound_stock", sa.Integer()),
        sa.Column("storage_fee_month", sa.Numeric(12, 2)),
        sa.Column("expiry_1_30", sa.Integer()),
        sa.Column("expiry_31_45", sa.Integer()),
        sa.Column("expiry_46_60", sa.Integer()),
        sa.Column("expiry_61_120", sa.Integer()),
        sa.Column("expiry_121_180", sa.Integer()),
        sa.Column("expiry_181_plus", sa.Integer()),
        sa.Column("recommendation", sa.Text()),
        sa.Column("raw", postgresql.JSONB()),
        sa.PrimaryKeyConstraint("snapshot_id", "coupang_option_id", name="pk_coupang_item"),
    )

    # --- wms snapshot -------------------------------------------------------
    op.create_table(
        "wms_inventory_snapshot",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("snapshot_date", sa.Date(), nullable=False, index=True),
        sa.Column("source_file", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    op.create_table(
        "wms_inventory_item",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "snapshot_id",
            sa.BigInteger(),
            sa.ForeignKey("wms_inventory_snapshot.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("barcode", sa.String(length=64), index=True),
        sa.Column("product_name", sa.Text()),
        sa.Column("loc_group", sa.Text()),
        sa.Column("loc", sa.Text()),
        sa.Column("total_qty", sa.Integer()),
        sa.Column("alloc_qty", sa.Integer()),
        sa.Column("available_qty", sa.Integer()),
        sa.Column("expiry_short", sa.Date()),
        sa.Column("expiry_long", sa.Date()),
        sa.Column("raw", postgresql.JSONB()),
    )

    # --- inbound plan -------------------------------------------------------
    op.create_table(
        "inbound_plan",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("plan_date", sa.Date(), nullable=False, index=True),
        sa.Column("fc_name", sa.String(length=32), nullable=False),
        sa.Column("worker", sa.String(length=64)),
        sa.Column(
            "coupang_snapshot_id",
            sa.BigInteger(),
            sa.ForeignKey("coupang_inventory_snapshot.id"),
        ),
        sa.Column(
            "wms_snapshot_id",
            sa.BigInteger(),
            sa.ForeignKey("wms_inventory_snapshot.id"),
        ),
        sa.Column("status", sa.String(length=16), server_default="draft", nullable=False),
        sa.Column("note", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("confirmed_at", sa.DateTime(timezone=True)),
    )

    op.create_table(
        "inbound_plan_item",
        sa.Column(
            "plan_id",
            sa.BigInteger(),
            sa.ForeignKey("inbound_plan.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("coupang_option_id", sa.BigInteger(), nullable=False),
        sa.Column("product_name", sa.Text()),
        sa.Column("option_name", sa.Text()),
        sa.Column("current_total_stock", sa.Integer()),
        sa.Column("sales_7d", sa.Integer()),
        sa.Column("sales_30d", sa.Integer()),
        sa.Column("sales_velocity_daily", sa.Numeric(12, 4)),
        sa.Column("stock_after_1w", sa.Numeric(12, 2)),
        sa.Column("stock_after_2w", sa.Numeric(12, 2)),
        sa.Column("stock_after_4w", sa.Numeric(12, 2)),
        sa.Column("box_qty", sa.Integer()),
        sa.Column("inbound_qty_suggested", sa.Integer()),
        sa.Column("inbound_qty_final", sa.Integer()),
        sa.Column("inbound_boxes", sa.Integer()),
        sa.Column("days_sellable_after", sa.Numeric(12, 2)),
        sa.Column("wms_short_expiry", sa.Date()),
        sa.Column("wms_long_expiry", sa.Date()),
        sa.Column("note", sa.Text()),
        sa.PrimaryKeyConstraint("plan_id", "coupang_option_id", name="pk_inbound_plan_item"),
    )

    # --- activity log -------------------------------------------------------
    op.create_table(
        "activity_log",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "ts", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("actor", sa.String(length=64)),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("entity", sa.String(length=64)),
        sa.Column("entity_id", sa.String(length=64)),
        sa.Column("detail", postgresql.JSONB()),
    )


def downgrade() -> None:
    op.drop_table("activity_log")
    op.drop_table("inbound_plan_item")
    op.drop_table("inbound_plan")
    op.drop_table("wms_inventory_item")
    op.drop_table("wms_inventory_snapshot")
    op.drop_table("coupang_inventory_item")
    op.drop_table("coupang_inventory_snapshot")
    op.drop_table("product_master")
