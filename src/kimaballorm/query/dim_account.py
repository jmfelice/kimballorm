from ..orm import DimAccount
from sqlalchemy import select, func


def get_active_accounts():
    return select(DimAccount).where(DimAccount.active == 1)


def get_specific_account(gl_account_id):
    return select(DimAccount).where(DimAccount.gl_account_id == gl_account_id)


def get_accounts_by_class(account_class):
    return select(DimAccount).where(DimAccount.account_class == account_class)


def get_category_counts():
    return select(
        DimAccount.gl_category,
        func.count(DimAccount.gl_account_id_key).label('account_count')
    ).group_by(DimAccount.gl_category)


def get_intercompany_accounts():
    return select(DimAccount).where(DimAccount.intercompany_flag == 1)


def get_accounts_with_description_like(description):
    return select(DimAccount).where(DimAccount.gl_account_description.ilike(f'%{description}%'))


def get_inactive_accounts():
    return select(DimAccount).where(DimAccount.active == 0)


def get_distinct_account_classes():
    return select(DimAccount.account_class).distinct()
