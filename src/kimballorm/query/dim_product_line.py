from ..orm import DimProductLine
from sqlalchemy import select, func
from sqlalchemy import and_, between, desc


def from_dim_product_line_get_all():
    return select(DimProductLine)


def from_dim_product_line_get_product_line(product_line):
    return (
        select(DimProductLine)
        .where(DimProductLine.product_line.in_(product_line))
    )


def from_dim_product_line_get_current_product_lines():
    return select(DimProductLine).where(DimProductLine.current_flag == 1)


def from_dim_product_line_get_history(product_line):
    return (
        select(DimProductLine)
        .where(DimProductLine.product_line == product_line)
        .order_by(DimProductLine.scd2_start_date)
    )


def from_dim_product_line_get_product_line_by_uom(uom):
    return select(DimProductLine).where(and_(DimProductLine.unit_of_measure == uom, DimProductLine.current_flag == 1))


def from_dim_product_line_get_product_line_added_after(date):
    return select(DimProductLine).where(and_(DimProductLine.scd2_start_date > date, DimProductLine.current_flag == 1))


def from_dim_product_line_get_product_line_changed_in_range(start_date, end_date):
    return select(DimProductLine).where(
        between(DimProductLine.scd2_start_date, start_date, end_date)
    )


def from_dim_product_line_get_product_line_by_description(description):
    return select(DimProductLine).where(
        and_(DimProductLine.product_line_description.ilike(f'%{description}%'), DimProductLine.current_flag == 1)
    )


def from_dim_product_line_get_product_line_count_by_uom():
    return (
        select(DimProductLine.unit_of_measure, func.count(DimProductLine.product_line_key).label('product_line_count'))
        .where(DimProductLine.current_flag == 1)
        .group_by(DimProductLine.unit_of_measure)
    )


def from_dim_product_line_get_inactive_product_lines():
    return select(DimProductLine).where(DimProductLine.active == 0)


def from_dim_product_line_get_product_line_with_multiple_versions():
    subquery = (
        select(DimProductLine.product_line, func.count().label('version_count'))
        .group_by(DimProductLine.product_line)
        .having(func.count() > 1)
        .subquery()
    )

    return (
        select(DimProductLine).join(subquery, DimProductLine.product_line == subquery.c.product_line)
        .where(DimProductLine.current_flag == 1)
    )


def from_dim_product_line_get_unchanged_product_lines():
    subquery = (
        select(DimProductLine.product_line, func.count().label('version_count'))
        .group_by(DimProductLine.product_line)
        .having(func.count() == 1)
        .subquery()
    )

    return select(DimProductLine).join(subquery, DimProductLine.product_line == subquery.c.product_line)


def from_dim_product_line_get_product_line_version_durations():
    return (
        select(
        DimProductLine.product_line,
        DimProductLine.product_line_description,
        DimProductLine.scd2_start_date,
        DimProductLine.scd2_end_date,
        func.date_diff('month', DimProductLine.scd2_start_date, DimProductLine.scd2_end_date).label("duration")
        )
        .order_by(DimProductLine.product_line, DimProductLine.scd2_start_date)
    )
