"""empty message

Revision ID: 3397b6edc5b2
Revises: d370fb7055d3
Create Date: 2024-08-20 09:21:07.203174

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3397b6edc5b2'
down_revision: Union[str, None] = 'd370fb7055d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('bridge_time_table_standard_source',
    sa.Column('bridge_time_table_standard', sa.Integer(), nullable=False, redshift_identity=(1, 1)),
    sa.Column('foreign_key_hash', sa.BIGINT(), nullable=False),
    sa.Column('attribute_hash', sa.BIGINT(), nullable=False),
    sa.Column('period_ending_key', sa.Integer(), nullable=True, redshift_distkey='period_ending_key'),
    sa.Column('duration_key', sa.Integer(), nullable=True),
    sa.Column('annum_key', sa.Integer(), nullable=True),
    sa.Column('start_date_key', sa.Integer(), nullable=True),
    sa.Column('end_date_key', sa.Integer(), nullable=True),
    sa.Column('weighted_business_days', sa.Numeric(precision=20, scale=8), nullable=True),
    sa.Column('action', sa.String(length=6), nullable=False),
    sa.PrimaryKeyConstraint('bridge_time_table_standard'),
    schema='finance_etl'
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('bridge_time_table_standard_source', schema='finance_etl')
    # ### end Alembic commands ###