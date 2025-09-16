"""increase_val_column_precision_to_30_2

Revision ID: 0b23c4b77941
Revises: 59c341f916c5
Create Date: 2025-09-13 19:04:14.637067

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0b23c4b77941"
down_revision: Union[str, None] = "59c341f916c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Increase precision of val column from DECIMAL(20,2) to DECIMAL(30,2)
    op.alter_column(
        "bronze_sec_facts",
        "val",
        existing_type=sa.Numeric(precision=20, scale=2),
        type_=sa.Numeric(precision=30, scale=2),
        nullable=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Revert val column back to DECIMAL(20,2)
    op.alter_column(
        "bronze_sec_facts",
        "val",
        existing_type=sa.Numeric(precision=30, scale=2),
        type_=sa.Numeric(precision=20, scale=2),
        nullable=False,
    )
