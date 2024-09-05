from ..orm import DimAccount
from ..orm import BridgeCategory
from sqlalchemy import select, func, or_


def from_dim_account_get_active_accounts():
    return select(DimAccount).where(DimAccount.active == 1)


def from_dim_account_get_account(gl_account_id):
    return select(DimAccount).where(DimAccount.gl_account_id.in_(gl_account_id))


def from_dim_account_get_account_class(account_class):
    return select(DimAccount).where(DimAccount.account_class == account_class)


def from_dim_account_get_count_by_category():
    return select(
        DimAccount.gl_category,
        func.count(DimAccount.gl_account_id_key).label('account_count')
    ).group_by(DimAccount.gl_category)


def from_dim_account_get_intercompany_accounts():
    return select(DimAccount).where(DimAccount.intercompany_flag == 1)


def from_dim_account_get_accounts_with_description_like(description):
    return select(DimAccount).where(DimAccount.gl_account_description.ilike(f'%{description}%'))


def from_dim_account_get_inactive_accounts():
    return select(DimAccount).where(DimAccount.active == 0)


def from_dim_account_get_count_by_class():
    return select(DimAccount.account_class).distinct()


def from_dim_account_get_category_sub_accounts(category):
    return (
        select(BridgeCategory.category, DimAccount.gl_account_id)
        .distinct()
        .select_from(DimAccount)
        .outerjoin(BridgeCategory, BridgeCategory.child_category == DimAccount.gl_category)
        .where(or_(BridgeCategory.category.in_(category), BridgeCategory.child_category.in_(category)))
        .order_by(BridgeCategory.category_order, DimAccount.gl_account_id)
    )
