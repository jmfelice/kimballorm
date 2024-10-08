"""Added active columns to duration and annum

Revision ID: d3484598c542
Revises: cb4cfa3967fe
Create Date: 2024-09-18 09:32:11.338043

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd3484598c542'
down_revision: Union[str, None] = 'cb4cfa3967fe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dim_annum', sa.Column('active', sa.Integer(), nullable=True), schema='finance_dw')
    op.add_column('dim_duration', sa.Column('active', sa.Integer(), nullable=True), schema='finance_dw')
    op.add_column('dim_annum_source', sa.Column('active', sa.Integer(), nullable=True), schema='finance_etl')
    op.add_column('dim_duration_source', sa.Column('active', sa.Integer(), nullable=True), schema='finance_etl')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('dim_duration_source', 'active', schema='finance_etl')
    op.drop_column('dim_annum_source', 'active', schema='finance_etl')
    op.drop_column('dim_duration', 'active', schema='finance_dw')
    op.drop_column('dim_annum', 'active', schema='finance_dw')
    # ### end Alembic commands ###
