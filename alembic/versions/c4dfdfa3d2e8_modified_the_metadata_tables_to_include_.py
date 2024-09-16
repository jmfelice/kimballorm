"""Modified the metadata tables to include descriptions.

Revision ID: c4dfdfa3d2e8
Revises: 71c67c46676b
Create Date: 2024-09-09 13:22:20.784573

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4dfdfa3d2e8'
down_revision: Union[str, None] = '71c67c46676b'
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
    sa.Column('target_server', sa.String(length=50), nullable=True),
    sa.Column('target_database', sa.String(length=30), nullable=True),
    sa.Column('target_schema', sa.String(length=50), nullable=True),
    sa.Column('target_table', sa.String(length=50), nullable=True),
    sa.Column('active', sa.Integer(), nullable=True),
    sa.Column('sql_target_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_create_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_truncate_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_drop_statement', sa.String(length=10000), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    op.create_table('metadata_source_import_bls',
    sa.Column('id', sa.Integer(), nullable=False, redshift_identity=(0, 1)),
    sa.Column('source', sa.String(length=10), nullable=True, redshift_sortkey=True),
    sa.Column('series_type', sa.String(length=20), nullable=True),
    sa.Column('series_id', sa.String(length=20), nullable=True),
    sa.Column('series_title', sa.String(length=255), nullable=True),
    sa.Column('series_short_title', sa.String(length=50), nullable=True),
    sa.Column('target_server', sa.String(length=50), nullable=True),
    sa.Column('target_database', sa.String(length=30), nullable=True),
    sa.Column('target_schema', sa.String(length=50), nullable=True),
    sa.Column('target_table', sa.String(length=50), nullable=True),
    sa.Column('active', sa.Integer(), nullable=True),
    sa.Column('sql_target_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_create_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_truncate_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_drop_statement', sa.String(length=10000), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    op.create_table('metadata_source_import_flat_files',
    sa.Column('id', sa.Integer(), nullable=False, redshift_identity=(0, 1)),
    sa.Column('source_location', sa.String(length=100), nullable=True),
    sa.Column('source_name', sa.String(length=100), nullable=True),
    sa.Column('source_table_description', sa.String(length=10000), nullable=True),
    sa.Column('target_server', sa.String(length=50), nullable=True),
    sa.Column('target_database', sa.String(length=30), nullable=True),
    sa.Column('target_schema', sa.String(length=50), nullable=True),
    sa.Column('target_table', sa.String(length=50), nullable=True),
    sa.Column('active', sa.Integer(), nullable=True),
    sa.Column('sql_target_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_create_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_truncate_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_drop_statement', sa.String(length=10000), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    op.create_table('metadata_source_import_fred',
    sa.Column('id', sa.Integer(), nullable=False, redshift_identity=(0, 1)),
    sa.Column('source', sa.String(length=10), nullable=True, redshift_sortkey=True),
    sa.Column('series_type', sa.String(length=20), nullable=True),
    sa.Column('series_id', sa.String(length=20), nullable=True),
    sa.Column('series_title', sa.String(length=255), nullable=True),
    sa.Column('series_short_title', sa.String(length=50), nullable=True),
    sa.Column('target_server', sa.String(length=50), nullable=True),
    sa.Column('target_database', sa.String(length=30), nullable=True),
    sa.Column('target_schema', sa.String(length=50), nullable=True),
    sa.Column('target_table', sa.String(length=50), nullable=True),
    sa.Column('active', sa.Integer(), nullable=True),
    sa.Column('sql_target_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_create_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_truncate_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_drop_statement', sa.String(length=10000), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    op.create_table('metadata_source_import_iseries',
    sa.Column('id', sa.Integer(), nullable=False, redshift_identity=(0, 1)),
    sa.Column('version_id', sa.Integer(), nullable=True),
    sa.Column('source_schema', sa.String(length=50), nullable=True),
    sa.Column('source_table', sa.String(length=50), nullable=True),
    sa.Column('source_table_description', sa.String(length=10000), nullable=True),
    sa.Column('target_server', sa.String(length=30), nullable=True),
    sa.Column('target_database', sa.String(length=30), nullable=True),
    sa.Column('target_schema', sa.String(length=50), nullable=True),
    sa.Column('target_table', sa.String(length=50), nullable=True),
    sa.Column('active', sa.Integer(), nullable=True),
    sa.Column('sql_source_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_create_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_truncate_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_drop_statement', sa.String(length=10000), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=True),
    sa.Column('end_date', sa.Date(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    op.create_table('metadata_source_import_iseries_gl',
    sa.Column('id', sa.Integer(), nullable=False, redshift_identity=(0, 1)),
    sa.Column('version_id', sa.Integer(), nullable=True),
    sa.Column('source_schema', sa.String(length=50), nullable=True),
    sa.Column('source_table', sa.String(length=50), nullable=True),
    sa.Column('source_table_description', sa.String(length=10000), nullable=True),
    sa.Column('target_server', sa.String(length=30), nullable=True),
    sa.Column('target_database', sa.String(length=30), nullable=True),
    sa.Column('target_schema', sa.String(length=50), nullable=True),
    sa.Column('target_table', sa.String(length=50), nullable=True),
    sa.Column('active', sa.Integer(), nullable=True),
    sa.Column('sql_source_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_create_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_truncate_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_drop_statement', sa.String(length=10000), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=True),
    sa.Column('end_date', sa.Date(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    op.create_table('metadata_source_import_iseries_mhf',
    sa.Column('id', sa.Integer(), nullable=False, redshift_identity=(0, 1)),
    sa.Column('version_id', sa.Integer(), nullable=True),
    sa.Column('source_schema', sa.String(length=50), nullable=True),
    sa.Column('source_table', sa.String(length=50), nullable=True),
    sa.Column('source_table_description', sa.String(length=10000), nullable=True),
    sa.Column('target_server', sa.String(length=30), nullable=True),
    sa.Column('target_database', sa.String(length=30), nullable=True),
    sa.Column('target_schema', sa.String(length=50), nullable=True),
    sa.Column('target_table', sa.String(length=50), nullable=True),
    sa.Column('active', sa.Integer(), nullable=True),
    sa.Column('sql_source_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_select_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_create_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_truncate_statement', sa.String(length=10000), nullable=True),
    sa.Column('sql_target_drop_statement', sa.String(length=10000), nullable=True),
    sa.Column('start_date', sa.Date(), nullable=True),
    sa.Column('end_date', sa.Date(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    schema='finance_meta'
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('metadata_source_import_iseries_mhf', schema='finance_meta')
    op.drop_table('metadata_source_import_iseries_gl', schema='finance_meta')
    op.drop_table('metadata_source_import_iseries', schema='finance_meta')
    op.drop_table('metadata_source_import_fred', schema='finance_meta')
    op.drop_table('metadata_source_import_flat_files', schema='finance_meta')
    op.drop_table('metadata_source_import_bls', schema='finance_meta')
    op.drop_table('metadata_source_import_bea', schema='finance_meta')
    # ### end Alembic commands ###
