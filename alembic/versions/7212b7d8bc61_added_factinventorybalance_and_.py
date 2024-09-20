"""Added FactInventoryBalance and FactInventoryChange

Revision ID: 7212b7d8bc61
Revises: 7b32d7cb5bb6
Create Date: 2024-09-16 11:02:23.481991

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7212b7d8bc61'
down_revision: Union[str, None] = '7b32d7cb5bb6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('fact_inventory_balance',
    sa.Column('fact_inventory_balance_key', sa.Integer(), nullable=False, redshift_identity=(1, 1)),
    sa.Column('foreign_key_hash', sa.BIGINT(), nullable=False),
    sa.Column('measures_hash', sa.BIGINT(), nullable=False),
    sa.Column('posting_date_key', sa.Integer(), nullable=True),
    sa.Column('branch_key', sa.Integer(), nullable=True),
    sa.Column('product_line_key', sa.Integer(), nullable=True),
    sa.Column('part_id_key', sa.Integer(), nullable=True),
    sa.Column('quantity', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.Column('store_cost_balance', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.Column('core_cost_balance', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.Column('warehouse_cost_balance', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.PrimaryKeyConstraint('fact_inventory_balance_key'),
    schema='finance_dw',
    redshift_interleaved_sortkey=('posting_date_key', 'branch_key', 'product_line_key', 'part_id_key')
    )
    op.create_table('fact_inventory_change',
    sa.Column('fact_inventory_change_key', sa.Integer(), nullable=False, redshift_identity=(1, 1)),
    sa.Column('foreign_key_hash', sa.BIGINT(), nullable=False),
    sa.Column('measures_hash', sa.BIGINT(), nullable=False),
    sa.Column('posting_date_key', sa.Integer(), nullable=True),
    sa.Column('branch_key', sa.Integer(), nullable=True),
    sa.Column('product_line_key', sa.Integer(), nullable=True),
    sa.Column('part_id_key', sa.Integer(), nullable=True),
    sa.Column('quantity', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.Column('store_cost_change', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.Column('core_cost_change', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.Column('warehouse_cost_change', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.PrimaryKeyConstraint('fact_inventory_change_key'),
    schema='finance_dw',
    redshift_interleaved_sortkey=('posting_date_key', 'branch_key', 'product_line_key', 'part_id_key')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('fact_inventory_change', schema='finance_dw')
    op.drop_table('fact_inventory_balance', schema='finance_dw')
    # ### end Alembic commands ###