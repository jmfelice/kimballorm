"""Added tables to metadata schema.

Revision ID: 169c3f28db5c
Revises: 7f42731d0ae9
Create Date: 2024-09-03 16:04:20.264481

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '169c3f28db5c'
down_revision: Union[str, None] = '7f42731d0ae9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('metadata_source_import_bea',
    sa.Column('id', sa.Integer(), nullable=False, redshift_identity=(0, 1)),
    sa.Column('source', sa.String(length=10), nullable=True, redshift_sortkey=True),
    sa.Column('series_type', sa.String(length=20), nullable=True),
    sa.Column('series_id', sa.String(length=20), nullable=True),
    sa.Column('series_title', sa.String(length=255), nullable=True),
    sa.Column('series_short_title', sa.String(length=50), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('metadata_source_import_bea', schema='finance_meta')
    # ### end Alembic commands ###