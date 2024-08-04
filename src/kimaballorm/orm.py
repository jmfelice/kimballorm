from sqlalchemy import Column, UniqueConstraint, ForeignKeyConstraint
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


class DimAccountMixin(object):
    gl_account_id_key = Column(Integer, primary_key=True, nullable=False)
    gl_account_id = Column(String(10), primary_key=False, nullable=True)
    gl_account_description = Column(String(50), primary_key=False, nullable=True)
    account_class = Column(String(1), primary_key=False, nullable=True)
    gl_category = Column(String(50), primary_key=False, nullable=True)
    intercompany_flag = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
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
    account_class_key = Column(Integer, primary_key=True, nullable=False)
    account_class = Column(String(1), primary_key=False, nullable=True)
    account_class_description = Column(String(9), primary_key=False, nullable=True)
    account_class_order = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
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
    branch_key = Column(Integer, primary_key=True, nullable=False)
    branch = Column(Integer, primary_key=False, nullable=True)
    branch_abb = Column(String(30), primary_key=False, nullable=True)
    branch_name = Column(String(50), primary_key=False, nullable=True)
    branch_type = Column(String(30), primary_key=False, nullable=True)
    is_branch = Column(Integer, primary_key=False, nullable=True)
    corporation = Column(Integer, primary_key=False, nullable=True)
    manager = Column(String(50), primary_key=False, nullable=True)
    customer_account = Column(Integer, primary_key=False, nullable=True)
    region = Column(String(30), primary_key=False, nullable=True)
    zone = Column(String(30), primary_key=False, nullable=True)
    servicing_warehouse = Column(String(10), primary_key=False, nullable=True)
    alternate_warehouse = Column(String(10), primary_key=False, nullable=True)
    service_charge = Column(
        Numeric(precision=5, scale=3), primary_key=False, nullable=True
    )
    bank_id = Column(Integer, primary_key=False, nullable=True)
    bank_name = Column(String(60), primary_key=False, nullable=True)
    bank_minimum_balance = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=True
    )
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
    date_key = Column(Integer, primary_key=True, nullable=False)
    calendar_date = Column(Date, primary_key=False, nullable=True)
    year = Column(Integer, primary_key=False, nullable=True)
    quarter = Column(Integer, primary_key=False, nullable=True)
    month = Column(Integer, primary_key=False, nullable=True)
    name_of_day = Column(String, primary_key=False, nullable=True)
    day_of_month = Column(Integer, primary_key=False, nullable=True)
    day_of_year = Column(Integer, primary_key=False, nullable=True)
    week = Column(Integer, primary_key=False, nullable=True)
    day_of_week = Column(Integer, primary_key=False, nullable=True)
    holiday = Column(String, primary_key=False, nullable=True)
    weighted_value = Column(
        Numeric(precision=4, scale=3), primary_key=False, nullable=True
    )
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
    weighted_business_days_mtd = Column(
        Numeric(precision=8, scale=3), primary_key=False, nullable=True
    )
    weighted_business_days_qtd = Column(
        Numeric(precision=8, scale=3), primary_key=False, nullable=True
    )
    weighted_business_days_ytd = Column(
        Numeric(precision=8, scale=3), primary_key=False, nullable=True
    )
    weighted_business_days_ltm = Column(
        Numeric(precision=8, scale=3), primary_key=False, nullable=True
    )
    weighted_business_days_wtd = Column(
        Numeric(precision=8, scale=3), primary_key=False, nullable=True
    )
    active = Column(Integer, primary_key=False, nullable=False)
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
    category_key = Column(Integer, primary_key=True, nullable=False)
    category = Column(String(50), primary_key=False, nullable=True)
    parent_category = Column(String(50), primary_key=False, nullable=True)
    category_class = Column(String(1), primary_key=False, nullable=True)
    category_order = Column(Integer, primary_key=False, nullable=True)
    isleaf = Column(Integer, primary_key=False, nullable=True)
    level = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "category"},)


class DimCategory(Base, DimCategoryMixin, SyncSCD1):
    __tablename__ = "dim_category"
    __table_args__ = (
        UniqueConstraint("category"),
        {"schema": "finance_dw"},
    )

    @classmethod
    def get_source_entity(cls):
        return DimCategorySource


class DimCategorySource(Base, DimCategoryMixin, SyncSCD1):
    __tablename__ = "dim_category_source"
    __table_args__ = ({"schema": "finance_etl"},)


class DimCorporationMixin(object):
    corporation_key = Column(Integer, primary_key=True, nullable=False)
    corporation = Column(Integer, primary_key=False, nullable=True)
    corporation_name = Column(String(50), primary_key=False, nullable=True)
    corporation_abbr = Column(String(30), primary_key=False, nullable=True)
    elimination_branch = Column(Integer, primary_key=False, nullable=True)
    federal_id_number = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
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
    indirect_cash_flow_category_key = Column(Integer, primary_key=True, nullable=False)
    indirect_cash_flow_category = Column(String(50), primary_key=False, nullable=True)
    parent_indirect_cash_flow_category = Column(
        String(50), primary_key=False, nullable=True
    )
    indirect_cash_flow_category_order = Column(
        Integer, primary_key=False, nullable=True
    )
    isleaf = Column(Integer, primary_key=False, nullable=True)
    level = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
    __custom_info__ = (
        {"table_type": "SCD_1", "natural_key": "indirect_cash_flow_category"},
    )


class DimIndirectCashFlowCategory(Base, DimIndirectCashFlowCategoryMixin, SyncSCD1):
    __tablename__ = "dim_indirect_cash_flow_category"
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
    description_key = Column(Integer, primary_key=True, nullable=False)
    description = Column(String(100), primary_key=False, nullable=True)
    __table_args__ = (
        UniqueConstraint("description"),
        {"schema": "finance_dw"},
    )
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "description"},)


class DimJournalEntry(Base, UtilityBase):
    __tablename__ = "dim_journal_entry"
    journal_entry_id_key = Column(Integer, primary_key=True, nullable=False)
    journal_entry_id = Column(Numeric(precision=14, scale=0), primary_key=False, nullable=True)
    __table_args__ = (
        UniqueConstraint("journal_entry_id"),
        {"schema": "finance_dw"},
    )
    __custom_info__ = ({"table_type": "SCD_1", "natural_key": "journal_entry_id"},)


class DimProductLineMixin(object):
    product_line_key = Column(Integer, primary_key=True, nullable=False)
    product_line = Column(Integer, primary_key=False, nullable=True)
    product_line_description = Column(String(100), primary_key=False, nullable=True)
    unit_of_measure = Column(String(10), primary_key=False, nullable=True)
    scd2_start_date = Column(Date, primary_key=False, nullable=True)
    scd2_end_date = Column(Date, primary_key=False, nullable=True)
    current_flag = Column(Integer, primary_key=False, nullable=True)
    active = Column(Integer, primary_key=False, nullable=False)
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


class BridgeCategoryMixin(object):
    bridge_category_key = Column(
        Integer, primary_key=True, nullable=True, redshift_identity=(1, 1)
    )
    category_key = Column(Integer, primary_key=False, nullable=True)
    child_category_key = Column(Integer, primary_key=False, nullable=True)
    category = Column(String(50), primary_key=False, nullable=True)
    child_category = Column(String(50), primary_key=False, nullable=True)
    category_order = Column(Integer, primary_key=False, nullable=True)
    category_class = Column(String(1), primary_key=False, nullable=True)
    level = Column(Integer, primary_key=False, nullable=True)
    isleaf = Column(Integer, primary_key=False, nullable=True)
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeCategory(Base, BridgeCategoryMixin, SyncFact):
    __tablename__ = "bridge_category"
    __table_args__ = (
        ForeignKeyConstraint(
            ("category_key",), ["finance_dw.dim_category.category_key"]
        ),
        ForeignKeyConstraint(
            ("child_category_key",), ["finance_dw.dim_category.category_key"]
        ),
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
    bridge_indirect_cash_flow_category_key = Column(
        Integer, primary_key=True, nullable=True, redshift_identity=(1, 1)
    )
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=True)
    child_indirect_cash_flow_category_key = Column(
        Integer, primary_key=False, nullable=True
    )
    indirect_cash_flow_category = Column(String(50), primary_key=False, nullable=True)
    child_indirect_cash_flow_category = Column(
        String(50), primary_key=False, nullable=True
    )
    indirect_cash_flow_category_order = Column(
        Integer, primary_key=False, nullable=True
    )
    level = Column(Integer, primary_key=False, nullable=True)
    isleaf = Column(Integer, primary_key=False, nullable=True)
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeIndirectCashFlowCategory(Base, BridgeIndirectCashFlowCategoryMixin, SyncFact):
    __tablename__ = "bridge_indirect_cash_flow_category"
    __table_args__ = (
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",),
            [
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
    bridge_map_cash_flow_key = Column(
        Integer, primary_key=True, nullable=True, redshift_identity=(1, 1)
    )
    gl_account_id_key = Column(Integer, primary_key=False, nullable=True)
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=True)
    reverse = Column(Integer, primary_key=False, nullable=True)
    __custom_info__ = ({"table_type": "Bridge"},)


class BridgeMapCashFlow(Base, BridgeMapCashFlowMixin, SyncFact):
    __tablename__ = "bridge_map_cash_flow"
    __table_args__ = (
        ForeignKeyConstraint(
            ("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]
        ),
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",),
            [
                "finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"
            ],
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
    fact_acquisition_cash_flow_key = Column(
        Integer, primary_key=True, nullable=False, redshift_identity=(1, 1)
    )
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=False)
    cash_flow = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    __custom_info__ = ({"table_type": "Fact"},)


class FactAcquisitionCashFlow(Base, FactAcquisitionCashFlowMixin, SyncFact):
    __tablename__ = "fact_acquisition_cash_flow"
    __table_args__ = (
        ForeignKeyConstraint(
            ("posting_date_key",), ["finance_dw.dim_calendar.date_key"]
        ),
        ForeignKeyConstraint(
            ("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]
        ),
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",),
            [
                "finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"
            ],
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
    fact_balance_sheet_key = Column(
        Integer, primary_key=True, nullable=False, redshift_identity=(1, 1)
    )
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    category_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    debit_balance = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    credit_balance = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    balance = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    __custom_info__ = ({"table_type": "Fact"},)


class FactBalanceSheet(Base, FactBalanceSheetMixin, SyncFact):
    __tablename__ = "fact_balance_sheet"
    __table_args__ = (
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(
            ("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]
        ),
        ForeignKeyConstraint(
            ("category_key",), ["finance_dw.dim_category.category_key"]
        ),
        ForeignKeyConstraint(
            ("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]
        ),
        ForeignKeyConstraint(
            ("posting_date_key",), ["finance_dw.dim_calendar.date_key"]
        ),
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
    fact_cash_flow_key = Column(
        Integer, primary_key=True, nullable=False, redshift_identity=(1, 1)
    )
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    category_key = Column(Integer, primary_key=False, nullable=False)
    indirect_cash_flow_category_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    general_ledger = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    acquisition = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    cash_flow = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    __custom_info__ = ({"table_type": "Fact"},)


class FactCashFlow(Base, FactCashFlowMixin, SyncFact):
    __tablename__ = "fact_cash_flow"
    __table_args__ = (
        ForeignKeyConstraint(
            ("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]
        ),
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(
            ("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]
        ),
        ForeignKeyConstraint(
            ("category_key",), ["finance_dw.dim_category.category_key"]
        ),
        ForeignKeyConstraint(
            ("indirect_cash_flow_category_key",),
            [
                "finance_dw.dim_indirect_cash_flow_category.indirect_cash_flow_category_key"
            ],
        ),
        ForeignKeyConstraint(
            ("posting_date_key",), ["finance_dw.dim_calendar.date_key"]
        ),
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
    fact_general_ledger_key = Column(
        Integer, primary_key=True, nullable=False, redshift_identity=(1, 1)
    )
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    category_key = Column(Integer, primary_key=False, nullable=False)
    description_key = Column(Integer, primary_key=False, nullable=False)
    journal_entry_id_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    debit_amount = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    credit_amount = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    __custom_info__ = ({"table_type": "Fact"},)


class FactGeneralLedger(Base, FactGeneralLedgerMixin, SyncFact):
    __tablename__ = "fact_general_ledger"
    __table_args__ = (
        ForeignKeyConstraint(
            ("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]
        ),
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(
            ("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]
        ),
        ForeignKeyConstraint(
            ("category_key",), ["finance_dw.dim_category.category_key"]
        ),
        ForeignKeyConstraint(
            ("description_key",), ["finance_dw.dim_journal_description.description_key"]
        ),
        ForeignKeyConstraint(
            ("journal_entry_id_key",),
            ["finance_dw.dim_journal_entry.journal_entry_id_key"],
        ),
        ForeignKeyConstraint(
            ("posting_date_key",), ["finance_dw.dim_calendar.date_key"]
        ),
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
    fact_income_summary_key = Column(
        Integer, primary_key=True, nullable=False, redshift_identity=(1, 1)
    )
    foreign_key_hash = Column(BIGINT, primary_key=False, nullable=False)
    measures_hash = Column(BIGINT, primary_key=False, nullable=False)
    branch_key = Column(Integer, primary_key=False, nullable=False)
    gl_account_id_key = Column(Integer, primary_key=False, nullable=False)
    corporation_key = Column(Integer, primary_key=False, nullable=False)
    posting_date_key = Column(Integer, primary_key=False, nullable=False)
    debit_amount = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    credit_amount = Column(
        Numeric(precision=20, scale=8), primary_key=False, nullable=False
    )
    amount = Column(Numeric(precision=20, scale=8), primary_key=False, nullable=False)
    __custom_info__ = ({"table_type": "Fact"},)


class FactIncomeSummary(Base, FactIncomeSummaryMixin, SyncFact):
    __tablename__ = "fact_income_summary"
    __table_args__ = (
        ForeignKeyConstraint(("branch_key",), ["finance_dw.dim_branch.branch_key"]),
        ForeignKeyConstraint(
            ("gl_account_id_key",), ["finance_dw.dim_account.gl_account_id_key"]
        ),
        ForeignKeyConstraint(
            ("corporation_key",), ["finance_dw.dim_corporation.corporation_key"]
        ),
        ForeignKeyConstraint(
            ("posting_date_key",), ["finance_dw.dim_calendar.date_key"]
        ),
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


FactBalanceSheet.dim_calendar_rel = relationship(
    "DimCalendar", backref="fact_balance_sheet"
)
FactCashFlow.dim_account_rel = relationship("DimAccount", backref="fact_cash_flow")
FactIncomeSummary.dim_branch_rel = relationship(
    "DimBranch", backref="fact_income_summary"
)
FactGeneralLedger.dim_journal_entry_rel = relationship(
    "DimJournalEntry", backref="fact_general_ledger"
)
FactAcquisitionCashFlow.dim_corporation_rel = relationship(
    "DimCorporation", backref="fact_acquisition_cash_flow"
)
FactBalanceSheet.dim_corporation_rel = relationship(
    "DimCorporation", backref="fact_balance_sheet"
)
FactGeneralLedger.dim_category_rel = relationship(
    "DimCategory", backref="fact_general_ledger"
)
FactGeneralLedger.dim_calendar_rel = relationship(
    "DimCalendar", backref="fact_general_ledger"
)
FactBalanceSheet.dim_branch_rel = relationship(
    "DimBranch", backref="fact_balance_sheet"
)
FactCashFlow.dim_indirect_cash_flow_category_rel = relationship(
    "DimIndirectCashFlowCategory", backref="fact_cash_flow"
)
FactCashFlow.dim_calendar_rel = relationship("DimCalendar", backref="fact_cash_flow")
FactGeneralLedger.dim_journal_description_rel = relationship(
    "DimJournalDescription", backref="fact_general_ledger"
)
FactGeneralLedger.dim_corporation_rel = relationship(
    "DimCorporation", backref="fact_general_ledger"
)
FactCashFlow.dim_corporation_rel = relationship(
    "DimCorporation", backref="fact_cash_flow"
)
BridgeMapCashFlow.dim_account_rel = relationship(
    "DimAccount", backref="bridge_map_cash_flow"
)
FactGeneralLedger.dim_account_rel = relationship(
    "DimAccount", backref="fact_general_ledger"
)
FactIncomeSummary.dim_account_rel = relationship(
    "DimAccount", backref="fact_income_summary"
)
# BridgeCategory.dim_category_rel = relationship('DimCategory', backref='bridge_category')
FactCashFlow.dim_branch_rel = relationship("DimBranch", backref="fact_cash_flow")
# BridgeIndirectCashFlowCategory.dim_indirect_cash_flow_category_rel = relationship(
#     'DimIndirectCashFlowCategory', backref='bridge_indirect_cash_flow_category')
FactIncomeSummary.dim_corporation_rel = relationship(
    "DimCorporation", backref="fact_income_summary"
)
FactBalanceSheet.dim_category_rel = relationship(
    "DimCategory", backref="fact_balance_sheet"
)
FactAcquisitionCashFlow.dim_indirect_cash_flow_category_rel = relationship(
    "DimIndirectCashFlowCategory", backref="fact_acquisition_cash_flow"
)
BridgeMapCashFlow.dim_indirect_cash_flow_category_rel = relationship(
    "DimIndirectCashFlowCategory", backref="bridge_map_cash_flow"
)
FactBalanceSheet.dim_account_rel = relationship(
    "DimAccount", backref="fact_balance_sheet"
)
FactCashFlow.dim_category_rel = relationship("DimCategory", backref="fact_cash_flow")
FactIncomeSummary.dim_calendar_rel = relationship(
    "DimCalendar", backref="fact_income_summary"
)
FactGeneralLedger.dim_branch_rel = relationship(
    "DimBranch", backref="fact_general_ledger"
)
FactAcquisitionCashFlow.dim_calendar_rel = relationship(
    "DimCalendar", backref="fact_acquisition_cash_flow"
)
