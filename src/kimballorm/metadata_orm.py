from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class MetadataSourceISeries(Base):
    __tablename__ = "metadata_source_import_iseries"
    id = Column(Integer, primary_key=True, nullable=False, redshift_identity=(0, 1))
    version_id = Column(Integer, primary_key = False, nullable = True)
    source_schema = Column(String(50), primary_key = False, nullable = True)
    source_table = Column(String(50), primary_key = False, nullable = True)
    source_table_description = Column(String(10000), primary_key = False, nullable = True)
    target_server = Column(String(30), primary_key = False, nullable = True)
    target_database = Column(String(30), primary_key = False, nullable = True)
    target_schema = Column(String(50), primary_key = False, nullable = True)
    target_table = Column(String(50), primary_key = False, nullable = True)
    active = Column(Integer, primary_key = False, nullable = True)
    sql_source_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_create_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_truncate_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_drop_statement = Column(String(10000), primary_key = False, nullable = True)
    start_date = Column(Date, primary_key = False, nullable = True)
    end_date = Column(Date, primary_key = False, nullable = True)
    __table_args__ = ({"schema": "finance_meta"},)


class MetadataSourceISeriesGL(Base):
    __tablename__ = "metadata_source_import_iseries_gl"
    id = Column(Integer, primary_key=True, nullable=False, redshift_identity=(0, 1))
    version_id = Column(Integer, primary_key = False, nullable = True)
    source_schema = Column(String(50), primary_key = False, nullable = True)
    source_table = Column(String(50), primary_key = False, nullable = True)
    source_table_description = Column(String(10000), primary_key = False, nullable = True)
    target_server = Column(String(30), primary_key = False, nullable = True)
    target_database = Column(String(30), primary_key = False, nullable = True)
    target_schema = Column(String(50), primary_key = False, nullable = True)
    target_table = Column(String(50), primary_key = False, nullable = True)
    active = Column(Integer, primary_key = False, nullable = True)
    sql_source_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_create_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_truncate_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_drop_statement = Column(String(10000), primary_key = False, nullable = True)
    start_date = Column(Date, primary_key = False, nullable = True)
    end_date = Column(Date, primary_key = False, nullable = True)
    __table_args__ = ({"schema": "finance_meta"},)


class MetadataSourceISeriesMHF(Base):
    __tablename__ = "metadata_source_import_iseries_mhf"
    id = Column(Integer, primary_key=True, nullable=False, redshift_identity=(0, 1))
    version_id = Column(Integer, primary_key = False, nullable = True)
    source_schema = Column(String(50), primary_key = False, nullable = True)
    source_table = Column(String(50), primary_key = False, nullable = True)
    source_table_description = Column(String(10000), primary_key = False, nullable = True)
    target_server = Column(String(30), primary_key = False, nullable = True)
    target_database = Column(String(30), primary_key = False, nullable = True)
    target_schema = Column(String(50), primary_key = False, nullable = True)
    target_table = Column(String(50), primary_key = False, nullable = True)
    active = Column(Integer, primary_key = False, nullable = True)
    sql_source_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_create_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_truncate_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_drop_statement = Column(String(10000), primary_key = False, nullable = True)
    start_date = Column(Date, primary_key = False, nullable = True)
    end_date = Column(Date, primary_key = False, nullable = True)
    __table_args__ = ({"schema": "finance_meta"},)


class MetadataSourceFlatFiles(Base):
    __tablename__ = "metadata_source_import_flat_files"
    id = Column(Integer, primary_key=True, nullable=False, redshift_identity=(0, 1))
    source_location = Column(String(100), primary_key = False, nullable = True)
    source_name = Column(String(100), primary_key = False, nullable = True)
    source_table_description = Column(String(10000), primary_key = False, nullable = True)
    target_server = Column(String(50), primary_key = False, nullable = True)
    target_database = Column(String(30), primary_key = False, nullable = True)
    target_schema = Column(String(50), primary_key = False, nullable = True)
    target_table = Column(String(50), primary_key = False, nullable = True)
    active = Column(Integer, primary_key = False, nullable = True)
    sql_target_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_create_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_truncate_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_drop_statement = Column(String(10000), primary_key = False, nullable = True)
    __table_args__ = ({"schema": "finance_meta"},)


class MetadataSourceFred(Base):
    __tablename__ = "metadata_source_import_fred"
    id = Column(Integer, primary_key=True, nullable=False, redshift_identity=(0, 1))
    source = Column(String(10), primary_key=False, nullable=True, redshift_sortkey = True)
    series_type = Column(String(20), primary_key=False, nullable=True)
    series_id = Column(String(20), primary_key=False, nullable=True)
    series_title = Column(String(255), primary_key=False, nullable=True)
    series_short_title = Column(String(50), primary_key=False, nullable=True)
    target_server = Column(String(50), primary_key = False, nullable = True)
    target_database = Column(String(30), primary_key = False, nullable = True)
    target_schema = Column(String(50), primary_key = False, nullable = True)
    target_table = Column(String(50), primary_key = False, nullable = True)
    active = Column(Integer, primary_key = False, nullable = True)
    sql_target_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_create_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_truncate_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_drop_statement = Column(String(10000), primary_key = False, nullable = True)
    __table_args__ = ({"schema": "finance_meta"},)


class MetadataSourceBEA(Base):
    __tablename__ = "metadata_source_import_bea"
    id = Column(Integer, primary_key=True, nullable=False, redshift_identity=(0, 1))
    source = Column(String(10), primary_key=False, nullable=True, redshift_sortkey = True)
    series_type = Column(String(20), primary_key=False, nullable=True)
    series_id = Column(String(20), primary_key=False, nullable=True)
    series_title = Column(String(255), primary_key=False, nullable=True)
    series_short_title = Column(String(50), primary_key=False, nullable=True)
    target_server = Column(String(50), primary_key = False, nullable = True)
    target_database = Column(String(30), primary_key = False, nullable = True)
    target_schema = Column(String(50), primary_key = False, nullable = True)
    target_table = Column(String(50), primary_key = False, nullable = True)
    active = Column(Integer, primary_key = False, nullable = True)
    sql_target_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_create_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_truncate_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_drop_statement = Column(String(10000), primary_key = False, nullable = True)
    __table_args__ = ({"schema": "finance_meta"},)


class MetadataSourceBLS(Base):
    __tablename__ = "metadata_source_import_bls"
    id = Column(Integer, primary_key=True, nullable=False, redshift_identity=(0, 1))
    source = Column(String(10), primary_key=False, nullable=True, redshift_sortkey = True)
    series_type = Column(String(20), primary_key=False, nullable=True)
    series_id = Column(String(20), primary_key=False, nullable=True)
    series_title = Column(String(255), primary_key=False, nullable=True)
    series_short_title = Column(String(50), primary_key=False, nullable=True)
    target_server = Column(String(50), primary_key = False, nullable = True)
    target_database = Column(String(30), primary_key = False, nullable = True)
    target_schema = Column(String(50), primary_key = False, nullable = True)
    target_table = Column(String(50), primary_key = False, nullable = True)
    active = Column(Integer, primary_key = False, nullable = True)
    sql_target_select_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_create_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_truncate_statement = Column(String(10000), primary_key = False, nullable = True)
    sql_target_drop_statement = Column(String(10000), primary_key = False, nullable = True)
    __table_args__ = ({"schema": "finance_meta"},)
