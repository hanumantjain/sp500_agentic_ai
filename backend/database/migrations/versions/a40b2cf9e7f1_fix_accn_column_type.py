"""fix_accn_column_type

Revision ID: a40b2cf9e7f1
Revises: ec6f4f3930e4
Create Date: 2025-09-12 17:04:44.303561

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision: str = "a40b2cf9e7f1"
down_revision: Union[str, None] = "ec6f4f3930e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Drop and recreate the table with correct accn type
    op.drop_table("bronze_sec_facts")

    # Recreate the table with String accn column
    op.create_table(
        "bronze_sec_facts",
        sa.Column("cik", sa.String(length=10), nullable=False),
        sa.Column("taxonomy", sa.String(length=64), nullable=False),
        sa.Column("tag", sa.String(length=128), nullable=False),
        sa.Column("unit", sa.String(length=32), nullable=False),
        sa.Column("val", sa.Numeric(), nullable=False),
        sa.Column("fy", sa.Numeric(), nullable=True),
        sa.Column("fp", sa.String(length=8), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("frame", sa.String(length=128), nullable=True),
        sa.Column("form", sa.String(length=16), nullable=True),
        sa.Column("filed", sa.Date(), nullable=False),
        sa.Column("accn", sa.String(length=32), nullable=False),  # Changed to String
        sa.PrimaryKeyConstraint("cik", "taxonomy", "tag", "unit", "filed", "accn"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Drop and recreate the table with original accn type
    op.drop_table("bronze_sec_facts")

    # Recreate the table with Numeric accn column
    op.create_table(
        "bronze_sec_facts",
        sa.Column("cik", sa.String(length=10), nullable=False),
        sa.Column("taxonomy", sa.String(length=64), nullable=False),
        sa.Column("tag", sa.String(length=128), nullable=False),
        sa.Column("unit", sa.String(length=32), nullable=False),
        sa.Column("val", sa.Numeric(), nullable=False),
        sa.Column("fy", sa.Numeric(), nullable=True),
        sa.Column("fp", sa.String(length=8), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("frame", sa.String(length=128), nullable=True),
        sa.Column("form", sa.String(length=16), nullable=True),
        sa.Column("filed", sa.Date(), nullable=False),
        sa.Column("accn", sa.Numeric(), nullable=False),  # Back to Numeric
        sa.PrimaryKeyConstraint("cik", "taxonomy", "tag", "unit", "filed", "accn"),
    )
