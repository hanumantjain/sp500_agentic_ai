"""fresh_start

Revision ID: 59c341f916c5
Revises:
Create Date: 2025-09-13 18:56:27.129453

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "59c341f916c5"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create bronze_sec_facts table
    op.create_table(
        "bronze_sec_facts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cik", sa.String(length=13), nullable=False),
        sa.Column("taxonomy", sa.String(length=64), nullable=False),
        sa.Column("tag", sa.String(length=256), nullable=False),
        sa.Column("unit", sa.String(length=32), nullable=False),
        sa.Column("val", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("fy", sa.Numeric(), nullable=True),
        sa.Column("fp", sa.String(length=8), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("frame", sa.String(length=128), nullable=True),
        sa.Column("form", sa.String(length=16), nullable=True),
        sa.Column("filed", sa.Date(), nullable=True),
        sa.Column("accn", sa.String(length=32), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create bronze_sec_facts_dict table
    op.create_table(
        "bronze_sec_facts_dict",
        sa.Column("taxonomy", sa.String(length=64), nullable=False),
        sa.Column("tag", sa.String(length=256), nullable=False),
        sa.Column("label", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("bronze_sec_facts_dict")
    op.drop_table("bronze_sec_facts")
