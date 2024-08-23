from ..orm import DimBranch
from sqlalchemy import select, func, and_, between, desc


def from_dim_branch_get_current_branches():
    return select(DimBranch).where(DimBranch.current_flag == 1)


def from_dim_branch_get_branch_history(branch_number):
    return select(DimBranch).where(DimBranch.branch == branch_number).order_by(DimBranch.scd2_start_date)


def from_dim_branch_get_branches_in_region(region):
    return select(DimBranch).where(and_(DimBranch.region == region, DimBranch.current_flag == 1))


def from_dim_branch_get_current_branches_of_type(branch_type):
    return select(DimBranch).where(and_(DimBranch.branch_type.in_(branch_type), DimBranch.current_flag == 1))


def from_dim_branch_get_branches_opened_after(date):
    return select(DimBranch).where(and_(DimBranch.open_date > date, DimBranch.current_flag == 1))


def from_dim_branch_get_branches_with_high_service_charge(threshold):
    return select(DimBranch).where(and_(DimBranch.service_charge > threshold, DimBranch.current_flag == 1))


def from_dim_branch_get_branch_from_manager(manager_name):
    return select(DimBranch).where(and_(DimBranch.manager.in_(manager_name), DimBranch.current_flag == 1))


def from_dim_branch_get_branches_in_state(state):
    return select(DimBranch).where(and_(DimBranch.state.in_(state), DimBranch.current_flag == 1))


def from_dim_branch_get_branches_from_primary_warehouse(warehouse):
    return (
        select(DimBranch.branch_key, DimBranch.branch)
        .where(and_(DimBranch.servicing_warehouse == warehouse, DimBranch.current_flag == 1))
    )


def from_dim_branch_get_branches_in_range(start_date, end_date):
    return select(DimBranch).where(
        between(DimBranch.scd2_start_date, start_date, end_date)
    )


def from_dim_branch_get_branches_by_corporation(corporation):
    return select(DimBranch).where(and_(DimBranch.corporation == corporation, DimBranch.current_flag == 1))


def from_dim_branch_get_branches_without_alternate_warehouse():
    return select(DimBranch).where(
        and_(DimBranch.alternate_warehouse.is_(None), DimBranch.current_flag == 1)
    )


def from_dim_branch_get_branch_count_by_region():
    return (
        select(DimBranch.region, func.count(DimBranch.branch_key).label('branch_count'))
        .where(DimBranch.current_flag == 1)
        .group_by(DimBranch.region)
    )


def from_dim_branch_get_branches_with_manager_changes():
    subquery = (
        select(DimBranch.branch, func.count(DimBranch.manager.distinct()).label('manager_count'))
        .group_by(DimBranch.branch)
        .having(func.count(DimBranch.manager.distinct()) > 1)
        .subquery()
    )

    return (
        select(DimBranch)
        .join(subquery, DimBranch.branch == subquery.c.branch)
        .where(DimBranch.current_flag == 1)
    )


def from_dim_branch_get_latest_change_for_each_branch():
    subquery = (
        select(DimBranch.branch, func.max(DimBranch.scd2_start_date).label('max_start_date'))
        .group_by(DimBranch.branch)
        .subquery()
    )

    return (
        select(DimBranch)
        .join(
            subquery,
            and_(DimBranch.branch == subquery.c.branch, DimBranch.scd2_start_date == subquery.c.max_start_date)
        ))
