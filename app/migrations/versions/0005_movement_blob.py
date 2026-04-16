"""store movement template file bytes in inbound_plan

Revision ID: 0005
Revises: 0004
Create Date: 2026-04-12

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "inbound_plan",
        sa.Column("movement_template_blob", postgresql.BYTEA(), nullable=True),
    )
    op.add_column(
        "inbound_plan",
        sa.Column("movement_template_filename", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("inbound_plan", "movement_template_filename")
    op.drop_column("inbound_plan", "movement_template_blob")
