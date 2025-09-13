"""recreate_bronze_sec_facts_fix_filed

Revision ID: 5f9a4e736efa
Revises: e2eba4272c68
Create Date: 2025-09-12 17:14:28.132966

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "5f9a4e736efa"
down_revision: Union[str, None] = "e2eba4272c68"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Drop and recreate the table with correct filed column
    op.drop_table("bronze_sec_facts")

    # Recreate the table with filed as nullable (not in primary key)
    op.create_table(
        "bronze_sec_facts",
        sa.Column("cik", sa.String(length=10), nullable=False),
        sa.Column("taxonomy", sa.String(length=64), nullable=False),
        sa.Column("tag", sa.String(length=128), nullable=False),
        sa.Column("unit", sa.String(length=32), nullable=False),
        sa.Column("val", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("fy", sa.Numeric(), nullable=True),
        sa.Column("fp", sa.String(length=8), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("frame", sa.String(length=128), nullable=True),
        sa.Column("form", sa.String(length=16), nullable=True),
        sa.Column("filed", sa.Date(), nullable=True),
        sa.Column("accn", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("cik", "taxonomy", "tag", "unit", "accn"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Drop and recreate the table with original filed column in primary key
    op.drop_table("bronze_sec_facts")

    # Recreate the table with filed in primary key
    op.create_table(
        "bronze_sec_facts",
        sa.Column("cik", sa.String(length=10), nullable=False),
        sa.Column("taxonomy", sa.String(length=64), nullable=False),
        sa.Column("tag", sa.String(length=128), nullable=False),
        sa.Column("unit", sa.String(length=32), nullable=False),
        sa.Column("val", sa.Numeric(precision=20, scale=2), nullable=False),
        sa.Column("fy", sa.Numeric(), nullable=True),
        sa.Column("fp", sa.String(length=8), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("frame", sa.String(length=128), nullable=True),
        sa.Column("form", sa.String(length=16), nullable=True),
        sa.Column("filed", sa.Date(), nullable=False),
        sa.Column("accn", sa.String(length=32), nullable=False),
        sa.PrimaryKeyConstraint("cik", "taxonomy", "tag", "unit", "filed", "accn"),
    )
