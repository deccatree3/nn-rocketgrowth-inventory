"""relax inbound_plan FC/plan_date constraints (draft 단계에서는 NULL 허용)

Revision ID: 0004
Revises: 0003
Create Date: 2026-04-12

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 검수 단계에서 채워질 컬럼들이라 draft 시점에는 NULL 허용
    op.alter_column("inbound_plan", "fc_name", existing_type=sa.String(length=32), nullable=True)
    op.alter_column("inbound_plan", "plan_date", existing_type=sa.Date(), nullable=True)


def downgrade() -> None:
    op.alter_column("inbound_plan", "fc_name", existing_type=sa.String(length=32), nullable=False)
    op.alter_column("inbound_plan", "plan_date", existing_type=sa.Date(), nullable=False)
