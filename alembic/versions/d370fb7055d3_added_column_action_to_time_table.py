"""Added column 'action' to time table

Revision ID: d370fb7055d3
Revises: d0b7997b37e8
Create Date: 2024-08-20 09:06:23.440276

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd370fb7055d3'
down_revision: Union[str, None] = 'd0b7997b37e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bridge_time_table_standard_source', sa.Column('action', sa.String(length=6), nullable=True), schema='finance_etl')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bridge_time_table_standard_source', 'action', schema='finance_etl')
    # ### end Alembic commands ###
