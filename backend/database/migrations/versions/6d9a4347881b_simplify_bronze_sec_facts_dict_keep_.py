"""simplify_bronze_sec_facts_dict_keep_audit_columns_only

Revision ID: 6d9a4347881b
Revises: 6c68b7b5f21a
Create Date: 2025-09-13 13:22:05.938043

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6d9a4347881b"
down_revision: Union[str, None] = "6c68b7b5f21a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Drop and recreate table with simplified structure
    op.execute("DROP TABLE bronze_sec_facts_dict;")

    # Create simplified table with only audit columns
    op.execute(
        """
        CREATE TABLE bronze_sec_facts_dict (
            id INT AUTO_INCREMENT PRIMARY KEY,
            taxonomy VARCHAR(64) NOT NULL,
            tag VARCHAR(256) NOT NULL,
            label TEXT NULL,
            description TEXT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_taxonomy_tag (taxonomy, tag)
        );
    """
    )


def downgrade() -> None:
    """Downgrade schema."""
    # Drop simplified table and recreate original
    op.execute("DROP TABLE bronze_sec_facts_dict;")

    # Create original table structure
    op.execute(
        """
        CREATE TABLE bronze_sec_facts_dict (
            taxonomy VARCHAR(64) NOT NULL,
            tag VARCHAR(256) NOT NULL,
            label TEXT NULL,
            description TEXT NULL,
            PRIMARY KEY (taxonomy, tag)
        );
    """
    )
