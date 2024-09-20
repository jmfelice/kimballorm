from sqlalchemy import Column, UniqueConstraint, ForeignKeyConstraint, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import Date
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy import BIGINT
from sqlalchemy.ext.declarative import declarative_base
from .mixin_utility import UtilityBase
from .mixin_fact_crud import SyncFact
from .mixin_scd1_crud import SyncSCD1
from .mixin_scd2_crud import SyncSCD2

Base = declarative_base()


class DimAnnumMixin(object):
    annum_key = Column(Integer, primary_key=True, nullable=False)
    primary_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    attribute_hash = Column(BIGINT, primary_key=False, nullable=False)
    annum = Column(String(30), primary_key=False, nullable=True)
    annum_description = Column(String(30), primary_key=False, nullable=True)
    annum_order = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=True)
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "annum"},)


class DimAnnum(Base, DimAnnumMixin, SyncSCD1):
    __tablename__ = "dim_annum"
    redshift_diststyle = "KEY"

    __table_args__ = (
        UniqueConstraint("annum"),
        {
            "schema": "finance_dw",
            "redshift_distkey": "annum_key",
            "redshift_sortkey": "annum"
        },
    )

    @classmethod
    def get_source_entity(cls):
        return DimAnnumSource


class DimAnnumSource(Base, DimAnnumMixin, SyncSCD1):
    __tablename__ = "dim_annum_source"
    action = Column(String(6), primary_key=False, nullable=True)
    __table_args__ = ({"schema": "finance_etl"},)


class DimDurationMixin(object):
    duration_key = Column(Integer, primary_key=True, nullable=False)
    primary_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    attribute_hash = Column(BIGINT, primary_key=False, nullable=False)
    duration = Column(String(3), primary_key=False, nullable=True)
    duration_description = Column(String(30), primary_key=False, nullable=True)
    duration_order = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=True)
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "duration"},)


class DimDuration(Base, DimDurationMixin, SyncSCD1):
    __tablename__ = "dim_duration"
    redshift_diststyle = "KEY"

    __table_args__ = (
        UniqueConstraint("duration"),
        {
            "schema": "finance_dw",
            "redshift_distkey": "duration_key",
            "redshift_sortkey": "duration"
        },
    )

    @classmethod
    def get_source_entity(cls):
        return DimDurationSource


class DimDurationSource(Base, DimDurationMixin, SyncSCD1):
    __tablename__ = "dim_duration_source"
    action = Column(String(6), primary_key=False, nullable=True)
    __table_args__ = ({"schema": "finance_etl"},)


class DimAccountMixin(object):
    gl_account_id_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "gl_account_key")
    gl_account_id = Column(String(10), primary_key=False, nullable=True, redshift_sortkey = True)
    gl_account_description = Column(String(50), primary_key=False, nullable=True)
    account_class = Column(String(1), primary_key=False, nullable=True, redshift_sortkey = True)
    gl_category = Column(String(50), primary_key=False, nullable=True)
    intercompany_flag = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "gl_account_id"},)


class DimAccount(Base, DimAccountMixin, SyncSCD1):
    __tablename__ = "dim_account"
    __table_args__ = (
        UniqueConstraint("gl_account_id"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimAccountSource


class DimAccountSource(Base, DimAccountMixin, SyncSCD1):
    __tablename__ = "dim_account_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimAccountClassMixin(object):
    account_class_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "account_class_key")
    account_class = Column(String(1), primary_key=False, nullable=True, redshift_sortkey = True)
    account_class_description = Column(String(9), primary_key=False, nullable=True)
    account_class_order = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": ["account_class"]},)


class DimAccountClass(Base, DimAccountClassMixin, SyncSCD1):
    __tablename__ = "dim_account_class"
    __table_args__ = (
        UniqueConstraint("account_class"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimAccountClassSource


class DimAccountClassSource(Base, DimAccountClassMixin, SyncSCD1):
    __tablename__ = "dim_account_class_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimBranchMixin(object):
    branch_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "branch_key")
    branch = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    branch_abb = Column(String(30), primary_key=False, nullable=True)
    branch_name = Column(String(50), primary_key=False, nullable=True)
    branch_type = Column(String(30), primary_key=False, nullable=True, redshift_sortkey = True)
    is_branch = Column(Integer, primary_key=False, nullable=True)
    corporation = Column(Integer, primary_key=False, nullable=True)
    manager = Column(String(50), primary_key=False, nullable=True, redshift_sortkey = True)
    customer_account = Column(Integer, primary_key=False, nullable=True)
    region = Column(String(30), primary_key=False, nullable=True, redshift_sortkey = True)
    zone = Column(String(30), primary_key=False, nullable=True, redshift_sortkey = True)
    servicing_warehouse = Column(String(10), primary_key=False, nullable=True)
    alternate_warehouse = Column(String(10), primary_key=False, nullable=True)
    service_charge = Column(Numeric(precision=5, scale=3), primary_key=False, nullable=True)
    bank_id = Column(Integer, primary_key=False, nullable=True)
    bank_name = Column(String(60), primary_key=False, nullable=True)
    bank_minimum_balance = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=True)
    street = Column(String(100), primary_key=False, nullable=True)
    city = Column(String(50), primary_key=False, nullable=True)
    state = Column(String(50), primary_key=False, nullable=True)
    zipcode = Column(Integer, primary_key=False, nullable=True)
    address = Column(String(200), primary_key=False, nullable=True)
    open_date = Column(Date, primary_key=False, nullable=True)
    close_date = Column(Date, primary_key=False, nullable=True)
    start_of_first_full_month = Column(Date, primary_key=False, nullable=True)
    scd2_start_date = Column(Date, primary_key=False, nullable=True)
    scd2_end_date = Column(Date, primary_key=False, nullable=True)
    current_flag = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = ({"table_type": "SCD_2", "natural_key": "branch"},)


class DimBranch(Base, DimBranchMixin, SyncSCD2):
    __tablename__ = "dim_branch"
    __table_args__ = (
        UniqueConstraint("branch", "scd2_start_date", "scd2_end_date"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimBranchSource


class DimBranchSource(Base, DimBranchMixin, SyncSCD2):
    __tablename__ = "dim_branch_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimCalendarMixin(object):
    date_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "date_key")
    calendar_date = Column(Date, primary_key=False, nullable=True, redshift_sortkey = True)
    year = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    quarter = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    month = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    name_of_day = Column(String, primary_key=False, nullable=True)
    day_of_month = Column(Integer, primary_key=False, nullable=True)
    day_of_year = Column(Integer, primary_key=False, nullable=True)
    week = Column(Integer, primary_key=False, nullable=True)
    day_of_week = Column(Integer, primary_key=False, nullable=True)
    holiday = Column(String, primary_key=False, nullable=True)
    weighted_value = Column(Numeric(precision=4, scale=3), primary_key=False, nullable=True)
    days_in_month = Column(Integer, primary_key=False, nullable=True)
    name_of_month = Column(String, primary_key=False, nullable=True)
    name_of_month_abb = Column(String, primary_key=False, nullable=True)
    first_date_of_month = Column(Date, primary_key=False, nullable=True)
    first_date_of_year = Column(Date, primary_key=False, nullable=True)
    first_date_of_quarter = Column(Date, primary_key=False, nullable=True)
    first_date_of_ltm = Column(Date, primary_key=False, nullable=True)
    py_date = Column(Date, primary_key=False, nullable=True)
    py_year = Column(Integer, primary_key=False, nullable=True)
    py_month = Column(Integer, primary_key=False, nullable=True)
    pm_date = Column(Date, primary_key=False, nullable=True)
    pm_year = Column(Integer, primary_key=False, nullable=True)
    pm_month = Column(Integer, primary_key=False, nullable=True)
    fiscal_year = Column(Integer, primary_key=False, nullable=True)
    fiscal_week = Column(Integer, primary_key=False, nullable=True)
    day_of_fiscal_year = Column(Integer, primary_key=False, nullable=True)
    day_frequency_of_fiscal_year = Column(Integer, primary_key=False, nullable=True)
    first_date_of_fiscal_week = Column(Date, primary_key=False, nullable=True)
    last_date_of_fiscal_week = Column(Date, primary_key=False, nullable=True)
    py_fiscal_year = Column(Integer, primary_key=False, nullable=True)
    cs_py_daily_date = Column(Date, primary_key=False, nullable=True)
    cs_py_daily_fiscal_year = Column(Integer, primary_key=False, nullable=True)
    cs_py_daily_fiscal_week = Column(Integer, primary_key=False, nullable=True)
    first_date_of_cs_py_fiscal_week = Column(Date, primary_key=False, nullable=True)
    weighted_business_days_mtd = Column(Numeric(precision=8, scale=3), primary_key=False, nullable=True)
    weighted_business_days_qtd = Column(Numeric(precision=8, scale=3), primary_key=False, nullable=True)
    weighted_business_days_ytd = Column(Numeric(precision=8, scale=3), primary_key=False, nullable=True)
    weighted_business_days_ltm = Column(Numeric(precision=8, scale=3), primary_key=False, nullable=True)
    weighted_business_days_wtd = Column(Numeric(precision=8, scale=3), primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "calendar_date"},)


class DimCalendar(Base, DimCalendarMixin, SyncSCD1):
    __tablename__ = "dim_calendar"
    __table_args__ = (
        UniqueConstraint("calendar_date"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimCalendarSource


class DimCalendarSource(Base, DimCalendarMixin, SyncSCD1):
    __tablename__ = "dim_calendar_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimCategoryMixin(object):
    category_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "category_key")
    category = Column(String(50), primary_key=False, nullable=True, redshift_sortkey = True)
    parent_category = Column(String(50), primary_key=False, nullable=True)
    category_class = Column(String(1), primary_key=False, nullable=True)
    category_order = Column(Integer, primary_key=False, nullable=True)
    isleaf = Column(Integer, primary_key=False, nullable=True)
    level = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "category"},)


class DimCategory(Base, DimCategoryMixin, SyncSCD1):
    __tablename__ = "dim_category"

    # child_bridges = relationship(
    #     'BridgeCategory',
    #     foreign_keys = ["BridgeCategory.category_key"],
    #     back_populates = 'parent_category_rel'
    # )
    # parent_bridges = relationship(
    #     'BridgeCategory',
    #     foreign_keys = ["BridgeCategory.child_category_key"],
    #     back_populates = 'child_category_rel'
    # )
    # general_ledgers = relationship(
    #     "FactGeneralLedger",
    #     secondary = "finance_dw.bridge_category",
    #     primaryjoin = "DimCategory.category_key==BridgeCategory.category_key",
    #     secondaryjoin = "BridgeCategory.child_category_key==FactGeneralLedger.category_key",
    #     back_populates = "categories"
    # )
    # balance_sheets = relationship(
    #     "FactBalanceSheet",
    #     secondary = "finance_dw.bridge_category",
    #     primaryjoin = "DimCategory.category_key==BridgeCategory.category_key",
    #     secondaryjoin = "BridgeCategory.child_category_key==FactBalanceSheet.category_key",
    #     back_populates = "categories"
    # )
    # cash_flows = relationship(
    #     "FactCashFlow",
    #     secondary = "finance_dw.bridge_category",
    #     primaryjoin = "DimCategory.category_key==BridgeCategory.category_key",
    #     secondaryjoin = "BridgeCategory.child_category_key==FactCashFlow.category_key",
    #     back_populates = "categories"
    # )
    __table_args__ = (UniqueConstraint("category"), {"schema": "finance_dw"}, )

    @classmethod
    def get_source_entity(cls):
        return DimCategorySource


class DimCategorySource(Base, DimCategoryMixin, SyncSCD1):
    __tablename__ = "dim_category_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimCorporationMixin(object):
    corporation_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "corporation_key")
    corporation = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    corporation_name = Column(String(50), primary_key=False, nullable=True, redshift_sortkey = True)
    corporation_abbr = Column(String(30), primary_key=False, nullable=True)
    elimination_branch = Column(Integer, primary_key=False, nullable=True)
    federal_id_number = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "corporation"},)


class DimCorporation(Base, DimCorporationMixin, SyncSCD1):
    __tablename__ = "dim_corporation"
    __table_args__ = (
        UniqueConstraint("corporation"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimCorporationSource


class DimCorporationSource(Base, DimCorporationMixin, SyncSCD1):
    __tablename__ = "dim_corporation_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimIndirectCashFlowCategoryMixin(object):
    indirect_cash_flow_category_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "indirect_cash_flow_category_key")
    indirect_cash_flow_category = Column(String(50), primary_key=False, nullable=True, redshift_sortkey = True)
    parent_indirect_cash_flow_category = Column(String(50), primary_key=False, nullable=True)
    indirect_cash_flow_category_order = Column(Integer, primary_key=False, nullable=True)
    isleaf = Column(Integer, primary_key=False, nullable=True)
    level = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = (
        {"table_type": "SCD_1", "natural_key": "indirect_cash_flow_category"},
    )


class DimIndirectCashFlowCategory(Base, DimIndirectCashFlowCategoryMixin, SyncSCD1):
    __tablename__ = "dim_indirect_cash_flow_category"

    # child_bridges = relationship(
    #     'BridgeIndirectCashFlowCategory',
    #     foreign_keys = ["BridgeIndirectCashFlowCategory.indirect_cash_flow_category_key"],
    #     back_populates = 'parent_category_rel'
    # )
    # parent_bridges = relationship(
    #     'BridgeIndirectCashFlowCategory',
    #     foreign_keys = ["BridgeIndirectCashFlowCategory.child_indirect_cash_flow_category_key"],
    #     back_populates = 'child_category_rel'
    # )
    # cash_flows = relationship(
    #     "FactCashFlow",
    #     secondary = "finance_dw.bridge_indirect_cash_flow_category",
    #     primaryjoin = "DimIndirectCashFlowCategory.indirect_cash_flow_category_key==BridgeIndirectCashFlowCategory.indirect_cash_flow_category_key",
    #     secondaryjoin = "BridgeIndirectCashFlowCategory.child_indirect_cash_flow_category_key==FactCashFlow.indirect_cash_flow_category_key",
    #     back_populates = "indirect_cash_flow_categories"
    # )
    # acquired_cash_flows = relationship(
    #     "FactAcquisitionCashFlow",
    #     secondary = "finance_dw.bridge_indirect_cash_flow_category",
    #     primaryjoin = "IndirectCashFlowCategory.indirect_cash_flow_category_key==BridgeIndirectCashFlowCategory.indirect_cash_flow_category_key",
    #     secondaryjoin = "BridgeIndirectCashFlowCategory.child_indirect_cash_flow_category_key==FactAcquisitionCashFlow.indirect_cash_flow_category_key",
    #     back_populates = "acquired_cash_flow_categories"
    # )

    __table_args__ = (
        UniqueConstraint("indirect_cash_flow_category"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimIndirectCashFlowCategorySource


class DimIndirectCashFlowCategorySource(Base, DimIndirectCashFlowCategoryMixin, SyncSCD1):
    __tablename__ = "dim_indirect_cash_flow_category_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimJournalDescription(Base, UtilityBase):
    __tablename__ = "dim_journal_description"
    description_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "description_key")
    description = Column(String(100), primary_key=False, nullable=True, redshift_sortkey = True)
    redshift_diststyle = "KEY"
    __table_args__ = (
        UniqueConstraint("description"),
        {"schema": "finance_dw"},
    )
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "description"},)


class DimJournalEntry(Base, UtilityBase):
    __tablename__ = "dim_journal_entry"
    journal_entry_id_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "journal_entry_id_key")
    journal_entry_id = Column(Numeric(precision=14, scale=0), primary_key=False, nullable=True, redshift_sortkey = True)
    redshift_diststyle = "KEY"
    __table_args__ = (
        UniqueConstraint("journal_entry_id"),
        {"schema": "finance_dw"},
    )
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "journal_entry_id"},)


class DimProductLineMixin(object):
    product_line_key = Column(Integer, primary_key=True, nullable=False, redshift_distkey = "product_line_key")
    product_line = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    product_line_description = Column(String(100), primary_key=False, nullable=True)
    unit_of_measure = Column(String(10), primary_key=False, nullable=True)
    factor = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=True)
    scd2_start_date = Column(Date, primary_key=False, nullable=True)
    scd2_end_date = Column(Date, primary_key=False, nullable=True)
    current_flag = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    redshift_diststyle = "KEY"
    __custom_info__ = ({"table_type": "SCD_2", "natural_key": "product_line"},)


class DimProductLine(Base, DimProductLineMixin, SyncSCD2):
    __tablename__ = "dim_product_line"
    __table_args__ = (
        UniqueConstraint("product_line", "scd2_start_date", "scd2_end_date"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimProductLineSource


class DimProductLineSource(Base, DimProductLineMixin, SyncSCD2):
    __tablename__ = "dim_product_line_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimPartMixin(object):
    part_id_key = Column(Integer, primary_key=True, nullable=False)
    primary_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    attribute_hash = Column(BIGINT, primary_key=False, nullable=False)
    sku = Column(Numeric(precision = 22, scale = 0))
    part_id = Column(String(24))
    part_id_2 = Column(String(24))
    product_line = Column(Numeric(precision = 6, scale = 0))
    part_description = Column(String(400))
    part_description_specific = Column(String(160))
    part_category = Column(String(60))
    part_subcategory = Column(String(60))
    part_segment = Column(String(60))
    popularity = Column(String(4))
    master_installer_flag = Column(Integer)
    active = Column(String(6))
    __custom_info__ = ({"table_type": "SCD1"},)


class DimPart(Base, DimPartMixin, SyncSCD1):
    __tablename__ = "dim_part"
    __table_args__ = (
        {
            "schema": "finance_dw",
            "redshift_diststyle": "KEY",
            "redshift_distkey": "part_id_key",
            "redshift_interleaved_sortkey": ("part_id", "product_line", "part_category", "part_subcategory", "part_segment")
        },
    )

    @classmethod
    def get_source_entity(cls):
        return DimPartSource


class DimPartSource(Base, DimPartMixin, SyncSCD1):
    __tablename__ = "dim_part_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class BridgeTimeTableStandardMixin(object):
    bridge_time_table_standard = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1,1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    attribute_hash = Column(BIGINT, primary_key=False, nullable=False)
    period_ending_key = Column(Integer)
    duration_key = Column(Integer)
    annum_key = Column(Integer)
    start_date_key = Column(Integer)
    end_date_key = Column(Integer)
    weighted_business_days = Column(Numeric(precision=20, scale=8))
    redshift_diststyle = "AUTO"
    redshift_sortkey = [period_ending_key, duration_key, annum_key]
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeTimeTableStandard(Base, BridgeTimeTableStandardMixin, SyncSCD1):
    __tablename__ = "bridge_time_table_standard"

    # period_ending_key_rel = relationship('DimCalendar', foreign_keys = ["period_ending_key"])
    # duration_key_rel = relationship('DimDuration', foreign_keys = ["duration_key"])
    # annum_key_rel = relationship('DimAnnum', foreign_keys = ["annum_key"])
    # start_date_key_rel = relationship('DimCalendar', foreign_keys = ["start_date_key"])
    # end_date_key_rel = relationship('DimCalendar', foreign_keys = ["end_date_key"])

    __table_args__ = (
        ForeignKeyConstraint(("period_ending_key",), ["finance_dw.dim_calendar.date_key"]),
        ForeignKeyConstraint(("duration_key",), ["finance_dw.dim_duration.duration_key"]),
        ForeignKeyConstraint(("annum_key",), ["finance_dw.dim_annum.annum_key"]),
        ForeignKeyConstraint(("start_date_key",), ["finance_dw.dim_calendar.date_key"]),
        ForeignKeyConstraint(("end_date_key",), ["finance_dw.dim_calendar.date_key"]),
        UniqueConstraint("period_ending_key", "duration_key", "annum_key"),
        {
            "schema": "finance_dw",
            "redshift_interleaved_sortkey": ("period_ending_key", "duration_key", "annum_key")
        },
    )

    @classmethod
    def get_source_entity(cls):
        return BridgeTimeTableStandardSource


class BridgeTimeTableStandardSource(Base, BridgeTimeTableStandardMixin, SyncSCD1):
    __tablename__ = "bridge_time_table_standard_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class BridgeTimeTableCompMixin(object):
    bridge_time_table_comp = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1,1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    attribute_hash = Column(BIGINT, primary_key=False, nullable=False)
    branch_key = Column(Integer)
    period_ending_key = Column(Integer)
    duration_key = Column(Integer)
    annum_key = Column(Integer)
    start_date_key = Column(Integer)
    end_date_key = Column(Integer)
    weighted_business_days = Column(Numeric(precision=20, scale=8))
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeTimeTableComp(Base, BridgeTimeTableCompMixin, SyncFact):
    __tablename__ = "bridge_time_table_comp"

    __table_args__ = (
        UniqueConstraint("branch_key", "period_ending_key", "duration_key", "annum_key"),
        {
            "schema": "finance_dw",
            "redshift_interleaved_sortkey": ("branch_key", "period_ending_key", "duration_key", "annum_key")
        },
    )

    @classmethod
    def get_source_entity(cls):
        return BridgeTimeTableCompSource


class BridgeTimeTableCompSource(Base, BridgeTimeTableCompMixin, SyncFact):
    __tablename__ = "bridge_time_table_comp_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class BridgeCategoryMixin(object):
    bridge_category_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    category_key = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    child_category_key = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    category = Column(String(50), primary_key=False, nullable=True)
    child_category = Column(String(50), primary_key=False, nullable=True)
    category_order = Column(Integer, primary_key=False, nullable=True)
    category_class = Column(String(1), primary_key=False, nullable=True)
    level = Column(Integer, primary_key=False, nullable=True)
    isleaf = Column(Integer, primary_key=False, nullable=True)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeCategory(Base, BridgeCategoryMixin, SyncFact):
    __tablename__ = "bridge_category"

    # parent_category_rel = relationship('DimCategory', foreign_keys = ["category_key"], back_populates = 'child_bridges')
    # child_category_rel = relationship('DimCategory', foreign_keys = ["child_category_key"], back_populates = 'parent_bridges')

    __table_args__ = (
        ForeignKeyConstraint(("category_key",), ["finance_dw.dim_category.category_key"]),
        ForeignKeyConstraint(("child_category_key",), ["finance_dw.dim_category.category_key"]),
        UniqueConstraint("category_key", "child_category_key"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return BridgeCategorySource


class BridgeCategorySource(Base, BridgeCategoryMixin, SyncFact):
    __tablename__ = "bridge_category_source"
    __table_args__ = ({"schema": "finance_etl"},)


class BridgeIndirectCashFlowCategoryMixin(object):
    bridge_indirect_cash_flow_category_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    child_indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    indirect_cash_flow_category = Column(String(50), primary_key=False, nullable=True)
    child_indirect_cash_flow_category = Column(String(50), primary_key=False, nullable=True)
    indirect_cash_flow_category_order = Column(Integer, primary_key=False, nullable=True)
    level = Column(Integer, primary_key=False, nullable=True)
    isleaf = Column(Integer, primary_key=False, nullable=True)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeIndirectCashFlowCategory(Base, BridgeIndirectCashFlowCategoryMixin, SyncFact):
    __tablename__ = "bridge_indirect_cash_flow_category"

    # parent_category_rel = relationship('DimIndirectCashFlowCategory', foreign_keys = ["indirect_cash_flow_category_key"], back_populates = 'child_bridges')
    # child_category_rel = relationship('DimIndirectCashFlowCategory', foreign_keys = ["child_indirect_cash_flow_category_key"], back_populates = 'parent_bridges')

    __table_args__ = (
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",), [
                "finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"
            ],
        ),
        ForeignKeyConstraint(
            ("child_indirect_cash_flow_category_key",),
            [
                "finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"
            ],
        ),
        UniqueConstraint(
            "indirect_cash_flow_category_key", "child_indirect_cash_flow_category_key"
        ),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return BridgeIndirectCashFlowCategorySource


class BridgeIndirectCashFlowCategorySource(Base, BridgeIndirectCashFlowCategoryMixin, SyncFact):
    __tablename__ = "bridge_indirect_cash_flow_category_source"
    __table_args__ = ({"schema": "finance_etl"},)


class BridgeMapCashFlowMixin(object):
    bridge_map_cash_flow_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    gl_account_id_key = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=True, redshift_sortkey = True)
    reverse = Column(Integer, primary_key=False, nullable=True)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeMapCashFlow(Base, BridgeMapCashFlowMixin, SyncFact):
    __tablename__ = "bridge_map_cash_flow"

    gl_account_id = relationship("DimAccount", backref = "bridge_map_cash_flow")
    indirect_cash_flow_category = relationship("DimIndirectCashFlowCategory", backref = "bridge_map_cash_flow")

    __table_args__ = (
        ForeignKeyConstraint(("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]),
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",),
            ["finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"],
        ),
        UniqueConstraint("gl_account_id_key", "indirect_cash_flow_category_key"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return BridgeMapCashFlowSource


class BridgeMapCashFlowSource(Base, BridgeMapCashFlowMixin, SyncFact):
    __tablename__ = "bridge_map_cash_flow_source"
    __table_args__ = ({"schema": "finance_etl"},)


class FactAcquisitionCashFlowMixin(object):
    fact_acquisition_cash_flow_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=False)
    cash_flow = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Fact"},)


class FactAcquisitionCashFlow(Base, FactAcquisitionCashFlowMixin, SyncFact):
    __tablename__ = "fact_acquisition_cash_flow"

    # calendar = relationship("DimCalendar", backref = "fact_acquisition_cash_flow")
    # indirect_cash_flow_category = relationship("BridgeIndirectCashFlowCategory", backref = "fact_acquisition_cash_flow")
    # corporation = relationship("DimCorporation", backref = "fact_acquisition_cash_flow")
    # indirect_cash_flow_categories = relationship(
    #     "DimIndirectCashFlowCategory",
    #     secondary="finance_dw.bridge_indirect_cash_flow_category",
    #     primaryjoin="FactAcquisitionCashFlow.bridge_indirect_cash_flow_category_key==BridgeIndirectCashFlowCategory.child_bridge_indirect_cash_flow_category_key",
    #     secondaryjoin="BridgeIndirectCashFlowCategory.bridge_indirect_cash_flow_category_key==DimIndirectCashFlowCategory.bridge_indirect_cash_flow_category_key",
    #     back_populates="acquired_cash_flows"
    # )

    __table_args__ = (
        ForeignKeyConstraint(("posting_date_key",), ["finance_dw.dim_calendar.date_key"]),
        ForeignKeyConstraint(("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]),
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",),
            ["finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"],
        ),
        UniqueConstraint(
            "posting_date_key", "corporation_key", "indirect_cash_flow_category_key"
        ),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return FactAcquisitionCashFlowSource


class FactAcquisitionCashFlowSource(Base, FactAcquisitionCashFlowMixin, SyncFact):
    __tablename__ = "fact_acquisition_cash_flow_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class FactBalanceSheetMixin(object):
    fact_balance_sheet_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    category_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    debit_balance = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    credit_balance = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    balance = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Fact"},)


class FactBalanceSheet(Base, FactBalanceSheetMixin, SyncFact):
    __tablename__ = "fact_balance_sheet"
    # corporation = relationship("DimCorporation", backref = "fact_balance_sheet")
    # branch = relationship("DimBranch", backref = "fact_balance_sheet")
    # gl_account_id = relationship("DimAccount", backref = "fact_balance_sheet")
    # calendar = relationship("DimCalendar", backref = "fact_balance_sheet")
    # categories = relationship(
    #     "DimCategory",
    #     secondary="finance_dw.bridge_category",
    #     primaryjoin="FactBalanceSheet.category_key==BridgeCategory.child_category_key",
    #     secondaryjoin="BridgeCategory.category_key==DimCategory.category_key",
    #     back_populates="balance_sheets"
    # )
    __table_args__ = (
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]),
        ForeignKeyConstraint(("category_key",), ["finance_dw.dim_category.category_key"]),
        ForeignKeyConstraint(("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]),
        ForeignKeyConstraint(("posting_date_key",), ["finance_dw.dim_calendar.date_key"]),
        UniqueConstraint(
            "branch_key",
            "gl_account_id_key",
            "category_key",
            "corporation_key",
            "posting_date_key",
        ),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return FactBalanceSheetSource


class FactBalanceSheetSource(Base, FactBalanceSheetMixin, SyncFact):
    __tablename__ = "fact_balance_sheet_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class FactCashFlowMixin(object):
    fact_cash_flow_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    category_key = Column(Integer, primary_key=False, nullable=False)
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    general_ledger = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    acquisition = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    cash_flow = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Fact"},)


class FactCashFlow(Base, FactCashFlowMixin, SyncFact):
    __tablename__ = "fact_cash_flow"
    # branch = relationship("DimBranch", backref = "fact_cash_flow", foreign_keys = ["branch_key"])
    # indirect_cash_flow_category = relationship("BridgeIndirectCashFlowCategory", backref = "fact_cash_flow", foreign_keys = ["indirect_cash_flow_category_key"])
    # calendar = relationship("DimCalendar", backref = "fact_cash_flow", foreign_keys = ["posting_date_key"])
    # corporation = relationship("DimCorporation", backref = "fact_cash_flow", foreign_keys = ["corporation_key"])
    # gl_account_id = relationship("DimAccount", backref = "fact_cash_flow", foreign_keys = ["gl_account_id_key"])
    # categories = relationship(
    #     "DimCategory",
    #     secondary="finance_dw.bridge_category",
    #     primaryjoin="FactCashFlow.category_key==BridgeCategory.child_category_key",
    #     secondaryjoin="BridgeCategory.category_key==DimCategory.category_key",
    #     back_populates="cash_flows"
    # )
    # indirect_cash_flow_categories = relationship(
    #     "DimIndirectCashFlowCategory",
    #     secondary="finance_dw.bridge_indirect_cash_flow_category",
    #     primaryjoin="FactCashFlow.bridge_indirect_cash_flow_category_key==BridgeIndirectCashFlowCategory.child_bridge_indirect_cash_flow_category_key",
    #     secondaryjoin="BridgeIndirectCashFlowCategory.bridge_indirect_cash_flow_category_key==DimIndirectCashFlowCategory.bridge_indirect_cash_flow_category_key",
    #     back_populates="cash_flows"
    # )
    __table_args__ = (
        ForeignKeyConstraint(("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]),
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]),
        ForeignKeyConstraint(("category_key",), ["finance_dw.dim_category.category_key"]),
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",),
            ["finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"],
        ),
        ForeignKeyConstraint(("posting_date_key",), ["finance_dw.dim_calendar.date_key"]),
        UniqueConstraint(
            "gl_account_id_key",
            "branch_key",
            "corporation_key",
            "category_key",
            "indirect_cash_flow_category_key",
            "posting_date_key",
        ),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return FactCashFlowSource


class FactCashFlowSource(Base, FactCashFlowMixin, SyncFact):
    __tablename__ = "fact_cash_flow_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class FactGeneralLedgerMixin(object):
    fact_general_ledger_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    category_key = Column(Integer, primary_key=False, nullable=False)
    description_key = Column(Integer, primary_key=False, nullable=False)
    journal_entry_id_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    debit_amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    credit_amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Fact"},)


class FactGeneralLedger(Base, FactGeneralLedgerMixin, SyncFact):
    __tablename__ = "fact_general_ledger"

    __table_args__ = (
        ForeignKeyConstraint(("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]),
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]),
        ForeignKeyConstraint(("category_key",), ["finance_dw.dim_category.category_key"]),
        ForeignKeyConstraint(("description_key",), ["finance_dw.dim_journal_description.description_key"]),
        ForeignKeyConstraint(("journal_entry_id_key",),["finance_dw.dim_journal_entry.journal_entry_id_key"],),
        ForeignKeyConstraint(("posting_date_key",), ["finance_dw.dim_calendar.date_key"]),
        UniqueConstraint(
            "gl_account_id_key",
            "branch_key",
            "corporation_key",
            "category_key",
            "description_key",
            "journal_entry_id_key",
            "posting_date_key",
        ),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return FactGeneralLedgerSource


class FactGeneralLedgerSource(Base, FactGeneralLedgerMixin, SyncFact):
    __tablename__ = "fact_general_ledger_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class FactIncomeSummaryMixin(object):
    fact_income_summary_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1, 1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    debit_amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    credit_amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Fact"},)


class FactIncomeSummary(Base, FactIncomeSummaryMixin, SyncFact):
    __tablename__ = "fact_income_summary"
    # gl_acount_id = relationship("DimAccount", backref = "fact_income_summary", foreign_keys=["gl_account_id_key"])
    # corporation = relationship("DimCorporation", backref = "fact_income_summary", foreign_keys=["corporation_key"])
    # calendar = relationship("DimCalendar", backref = "fact_income_summary", foreign_keys=["posting_date_key"])
    # branch = relationship("DimBranch", backref = "fact_income_summary", foreign_keys=["branch_key"])
    __table_args__ = (
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]),
        ForeignKeyConstraint(("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]),
        ForeignKeyConstraint(("posting_date_key",), ["finance_dw.dim_calendar.date_key"]),
        UniqueConstraint(
            "branch_key", "gl_account_id_key", "corporation_key", "posting_date_key"
        ),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return FactIncomeSummarySource


class FactIncomeSummarySource(Base, FactIncomeSummaryMixin, SyncFact):
    __tablename__ = "fact_income_summary_source"
    action = Column(String(6), primary_key=False, nullable=False)
    __table_args__ = ({"schema": "finance_etl"},)


class FactInventoryBalanceMixin(object):
    fact_inventory_balance_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1,1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    posting_date_key = Column(Integer)
    branch_key = Column(Integer)
    product_line_key = Column(Integer)
    part_id_key = Column(Integer)
    quantity = Column(Numeric(20, 8))
    store_cost_balance = Column(Numeric(20, 8))
    core_cost_balance = Column(Numeric(20, 8))
    warehouse_cost_balance = Column(Numeric(20, 8))
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Fact"},)


class FactInventoryBalance(Base, FactInventoryBalanceMixin, SyncFact):
    __tablename__ = "fact_inventory_balance"
    __table_args__ = (
        {
            "schema": "finance_dw",
            "redshift_interleaved_sortkey": ("posting_date_key", "branch_key", "product_line_key", "part_id_key")
        },
    )


class FactInventoryChangeMixin(object):
    fact_inventory_change_key = Column(Integer, primary_key=True, nullable=False, redshift_identity=(1,1))
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    posting_date_key = Column(Integer)
    branch_key = Column(Integer)
    product_line_key = Column(Integer)
    part_id_key = Column(Integer)
    quantity = Column(Numeric(20, 8))
    store_cost_change = Column(Numeric(20, 8))
    core_cost_change = Column(Numeric(20, 8))
    warehouse_cost_change = Column(Numeric(20, 8))
    redshift_diststyle = "AUTO"
    __custom_info__ = ({"table_type": "Fact"},)


class FactInventorychange(Base, FactInventoryChangeMixin, SyncFact):
    __tablename__ = "fact_inventory_change"
    __table_args__ = (
        {
            "schema": "finance_dw",
            "redshift_interleaved_sortkey": ("posting_date_key", "branch_key", "product_line_key", "part_id_key")
        },
    )
