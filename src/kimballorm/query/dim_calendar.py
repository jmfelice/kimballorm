from ..orm import DimCalendar
from sqlalchemy import select, func, and_, or_, between
from datetime import date, timedelta


def from_dim_calendar_get_dates_in_year(year):
    return select(DimCalendar).where(DimCalendar.year == year)


def from_dim_calendar_get_holidays_in_year(year):
    return select(DimCalendar).where(
        and_(DimCalendar.year == year, DimCalendar.holiday.isnot(None))
    )


def from_dim_calendar_get_date_range(start_date, end_date):
    return select(DimCalendar).where(
        between(DimCalendar.calendar_date, start_date, end_date)
    )


def from_dim_calendar_get_fiscal_weeks_in_fiscal_year(fiscal_year):
    return select(DimCalendar.fiscal_week).distinct().where(
        DimCalendar.fiscal_year == fiscal_year
    ).order_by(DimCalendar.fiscal_week)


def from_dim_calendar_get_weighted_busines_days(calendar_date):
    return (
        select(
        DimCalendar.date_key,
        DimCalendar.calendar_date,
        DimCalendar.weighted_business_days_mtd,
        DimCalendar.weighted_business_days_qtd,
        DimCalendar.weighted_business_days_ytd,
        DimCalendar.weighted_business_days_ltm
        )
        .where(DimCalendar.calendar_date == calendar_date)
    )


def from_dim_calendar_get_dates_with_holiday(holiday_name):
    return select(DimCalendar).where(DimCalendar.holiday == holiday_name)


def from_dim_calendar_get_weekends_in_year(year):
    return select(DimCalendar).where(
        and_(DimCalendar.year == year, or_(DimCalendar.day_of_week == 6, DimCalendar.day_of_week == 7))
    )


def from_dim_calendar_get_end_of_month_dates(year = None):
    query = (
        select(DimCalendar.date_key, DimCalendar.calendar_date)
        .where(DimCalendar.calendar_date == DimCalendar.days_in_month)
    )
    if year is not None:
        query = query.where(DimCalendar.year == year)
    return query


def from_dim_calendar_get_end_of_month_series(calendar_date, months_back):
    query = (
        select(DimCalendar.calendar_date)
        .where(and_(
            DimCalendar.day_of_month == DimCalendar.days_in_month,
            between(
                DimCalendar.calendar_date,
                func.date_add("month", -(months_back-1), calendar_date),
                calendar_date
            )
        ))
        .order_by(DimCalendar.calendar_date)
    )
    return query
