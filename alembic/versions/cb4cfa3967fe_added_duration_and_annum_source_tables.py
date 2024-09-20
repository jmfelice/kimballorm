"""Added duration and annum source tables.

Revision ID: cb4cfa3967fe
Revises: 7212b7d8bc61
Create Date: 2024-09-18 09:17:33.926745

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cb4cfa3967fe'
down_revision: Union[str, None] = '7212b7d8bc61'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('dim_annum',
    sa.Column('annum_key', sa.Integer(), nullable=False),
    sa.Column('primary_key_hash', sa.BIGINT(), nullable=False),
    sa.Column('attribute_hash', sa.BIGINT(), nullable=False),
    sa.Column('annum', sa.String(length=30), nullable=True),
    sa.Column('annum_description', sa.String(length=30), nullable=True),
    sa.Column('annum_order', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('annum_key'),
    sa.UniqueConstraint('annum'),
    schema='finance_dw',
    redshift_distkey='annum_key',
    redshift_sortkey='annum'
    )
    op.create_table('dim_duration',
    sa.Column('duration_key', sa.Integer(), nullable=False),
    sa.Column('primary_key_hash', sa.BIGINT(), nullable=False),
    sa.Column('attribute_hash', sa.BIGINT(), nullable=False),
    sa.Column('duration', sa.String(length=3), nullable=True),
    sa.Column('duration_description', sa.String(length=30), nullable=True),
    sa.Column('duration_order', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('duration_key'),
    sa.UniqueConstraint('duration'),
    schema='finance_dw',
    redshift_distkey='duration_key',
    redshift_sortkey='duration'
    )
    op.create_table('dim_annum_source',
    sa.Column('annum_key', sa.Integer(), nullable=False),
    sa.Column('primary_key_hash', sa.BIGINT(), nullable=False),
    sa.Column('attribute_hash', sa.BIGINT(), nullable=False),
    sa.Column('annum', sa.String(length=30), nullable=True),
    sa.Column('annum_description', sa.String(length=30), nullable=True),
    sa.Column('annum_order', sa.Integer(), nullable=True),
    sa.Column('action', sa.String(length=6), nullable=True),
    sa.PrimaryKeyConstraint('annum_key'),
    schema='finance_etl'
    )
    op.create_table('dim_duration_source',
    sa.Column('duration_key', sa.Integer(), nullable=False),
    sa.Column('primary_key_hash', sa.BIGINT(), nullable=False),
    sa.Column('attribute_hash', sa.BIGINT(), nullable=False),
    sa.Column('duration', sa.String(length=3), nullable=True),
    sa.Column('duration_description', sa.String(length=30), nullable=True),
    sa.Column('duration_order', sa.Integer(), nullable=True),
    sa.Column('action', sa.String(length=6), nullable=True),
    sa.PrimaryKeyConstraint('duration_key'),
    schema='finance_etl'
    )
    op.create_foreign_key(None, 'bridge_time_table_standard', 'dim_duration', ['duration_key'], ['duration_key'], source_schema='finance_dw', referent_schema='finance_dw')
    op.create_foreign_key(None, 'bridge_time_table_standard', 'dim_annum', ['annum_key'], ['annum_key'], source_schema='finance_dw', referent_schema='finance_dw')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'bridge_time_table_standard', schema='finance_dw', type_='foreignkey')
    op.drop_constraint(None, 'bridge_time_table_standard', schema='finance_dw', type_='foreignkey')
    op.drop_table('dim_duration_source', schema='finance_etl')
    op.drop_table('dim_annum_source', schema='finance_etl')
    op.drop_table('dim_duration', schema='finance_dw')
    op.drop_table('dim_annum', schema='finance_dw')
    # ### end Alembic commands ###
