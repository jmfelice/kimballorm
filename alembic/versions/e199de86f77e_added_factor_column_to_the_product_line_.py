"""Added factor column to the product line dimension.

Revision ID: e199de86f77e
Revises: 3397b6edc5b2
Create Date: 2024-08-23 15:17:11.117647

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e199de86f77e'
down_revision: Union[str, None] = '3397b6edc5b2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dim_product_line', sa.Column('factor', sa.Numeric(precision=20, scale=8), nullable=True), schema='finance_dw')
    op.add_column('dim_product_line_source', sa.Column('factor', sa.Numeric(precision=20, scale=8), nullable=True), schema='finance_etl')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('dim_product_line_source', 'factor', schema='finance_etl')
    op.drop_column('dim_product_line', 'factor', schema='finance_dw')
    # ### end Alembic commands ###
