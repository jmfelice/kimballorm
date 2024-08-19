"""Add 2 dimensions: annuma and duration.  Both of these will be manually maintained.

Revision ID: 64618c04e010
Revises: 88c415701cd5
Create Date: 2024-08-19 08:53:28.664653

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '64618c04e010'
down_revision: Union[str, None] = '88c415701cd5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('dim_annum',
    sa.Column('annum_key', sa.Integer(), nullable=False, redshift_distkey='annum_key'),
    sa.Column('annum', sa.String(length=30), nullable=True, redshift_sortkey=True),
    sa.Column('annum_order', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('annum_key'),
    sa.UniqueConstraint('annum'),
    schema='finance_dw'
    )
    op.create_table('dim_duration',
    sa.Column('duration_key', sa.Integer(), nullable=False, redshift_distkey='duration_key'),
    sa.Column('duration', sa.String(length=3), nullable=True, redshift_sortkey=True),
    sa.Column('duration_order', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('duration_key'),
    sa.UniqueConstraint('duration'),
    schema='finance_dw'
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('dim_duration', schema='finance_dw')
    op.drop_table('dim_annum', schema='finance_dw')
    # ### end Alembic commands ###