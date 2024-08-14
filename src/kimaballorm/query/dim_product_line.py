from ..orm import DimProductLine
from sqlalchemy import select, func
from sqlalchemy import and_, between, desc


def get_product_lines(product_lines = None):
    if product_lines is None:
        return select(DimProductLine)
    return (
        select(DimProductLine)
        .where(DimProductLine.product_line.in_(product_lines))
    )


def get_current_product_lines():
    return select(DimProductLine).where(DimProductLine.current_flag == 1)


def get_product_line_history(product_line_number):
    return select(DimProductLine).where(DimProductLine.product_line == product_line_number).order_by(
        DimProductLine.scd2_start_date)


def get_product_lines_by_uom(uom):
    return select(DimProductLine).where(and_(DimProductLine.unit_of_measure == uom, DimProductLine.current_flag == 1))


def get_product_lines_added_after(date):
    return select(DimProductLine).where(and_(DimProductLine.scd2_start_date > date, DimProductLine.current_flag == 1))


def get_product_lines_changed_in_range(start_date, end_date):
    return select(DimProductLine).where(
        between(DimProductLine.scd2_start_date, start_date, end_date)
    )


def get_product_lines_by_description(description):
    return select(DimProductLine).where(
        and_(DimProductLine.product_line_description.ilike(f'%{description}%'), DimProductLine.current_flag == 1)
    )


def get_recently_changed_product_lines(limit = 10):
    return select(DimProductLine).order_by(desc(DimProductLine.scd2_start_date)).limit(limit)


def get_all_versions_of_product_line(product_line_number):
    return select(DimProductLine).where(DimProductLine.product_line == product_line_number).order_by(
        DimProductLine.scd2_start_date)


def get_product_line_count_by_uom():
    return (
        select(DimProductLine.unit_of_measure, func.count(DimProductLine.product_line_key).label('product_line_count'))
        .where(DimProductLine.current_flag == 1)
        .group_by(DimProductLine.unit_of_measure)
    )


def get_discontinued_product_lines():
    return select(DimProductLine).where(DimProductLine.scd2_end_date.isnot(None))


def get_latest_change_for_each_product_line():
    subquery = (
        select(DimProductLine.product_line, func.max(DimProductLine.scd2_start_date).label('max_start_date'))
        .group_by(DimProductLine.product_line)
        .subquery()
    )

    return (
        select(DimProductLine)
        .join(
            subquery,
            and_(
                DimProductLine.product_line == subquery.c.product_line,
                DimProductLine.scd2_start_date == subquery.c.max_start_date
            )))


def get_product_lines_with_multiple_versions():
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


def get_unchanged_product_lines():
    subquery = (
        select(DimProductLine.product_line, func.count().label('version_count'))
        .group_by(DimProductLine.product_line)
        .having(func.count() == 1)
        .subquery()
    )

    return select(DimProductLine).join(subquery, DimProductLine.product_line == subquery.c.product_line)


def get_product_lines_with_null_description():
    return select(DimProductLine).where(
        and_(DimProductLine.product_line_description.is_(None), DimProductLine.current_flag == 1)
    )


def get_product_line_version_durations():
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
