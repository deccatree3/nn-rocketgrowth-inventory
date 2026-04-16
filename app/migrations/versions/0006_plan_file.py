"""plan_file 테이블 추가 — 업로드 파일 영속화.

Revision ID: 0006
Revises: 0005
"""
from alembic import op
import sqlalchemy as sa

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "plan_file",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "plan_id",
            sa.BigInteger,
            sa.ForeignKey("inbound_plan.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("file_type", sa.String(32), nullable=False),
        sa.Column("file_name", sa.Text, nullable=False),
        sa.Column("content", sa.LargeBinary, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("plan_id", "file_type", name="uq_plan_file_type"),
    )


def downgrade():
    op.drop_table("plan_file")
