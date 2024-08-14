from ..orm import DimCalendar
from sqlalchemy import select, func, and_, or_, between
from datetime import date, timedelta


def get_dates_in_year(year):
    return select(DimCalendar).where(DimCalendar.year == year)


def get_dates_in_fiscal_year(fiscal_year):
    return select(DimCalendar).where(DimCalendar.fiscal_year == fiscal_year)


def get_dates_in_quarter(year, quarter):
    return select(DimCalendar).where(
        and_(DimCalendar.year == year, DimCalendar.quarter == quarter)
    )


def get_holidays_in_year(year):
    return select(DimCalendar).where(
        and_(DimCalendar.year == year, DimCalendar.holiday.isnot(None))
    )


def get_first_days_of_months(year):
    return select(DimCalendar).where(
        and_(DimCalendar.year == year, DimCalendar.day_of_month == 1)
    )


def get_mondays_in_year(year):
    return select(DimCalendar).where(
        and_(DimCalendar.year == year, DimCalendar.day_of_week == 1)
    )


def get_date_range(start_date, end_date):
    return select(DimCalendar).where(
        between(DimCalendar.calendar_date, start_date, end_date)
    )


def get_last_n_days(n):
    today = date.today()
    n_days_ago = today - timedelta(days = n)
    return select(DimCalendar).where(
        DimCalendar.calendar_date > n_days_ago
    ).order_by(DimCalendar.calendar_date)


def get_fiscal_weeks_in_fiscal_year(fiscal_year):
    return select(DimCalendar.fiscal_week).distinct().where(
        DimCalendar.fiscal_year == fiscal_year
    ).order_by(DimCalendar.fiscal_week)


def get_weighted_business_days_mtd():
    today = date.today()
    return select(DimCalendar.weighted_business_days_mtd).where(
        DimCalendar.calendar_date == today
    )


def get_dates_with_holiday(holiday_name):
    return select(DimCalendar).where(DimCalendar.holiday == holiday_name)


def get_weekends_in_year(year):
    return select(DimCalendar).where(
        and_(DimCalendar.year == year, or_(DimCalendar.day_of_week == 6, DimCalendar.day_of_week == 7))
    )


def get_last_days_of_months(year):
    subquery = select(func.max(DimCalendar.day_of_month)).where(
        and_(DimCalendar.year == year, DimCalendar.month == DimCalendar.month)
    ).group_by(DimCalendar.month).scalar_subquery()

    return select(DimCalendar).where(
        and_(DimCalendar.year == year, DimCalendar.day_of_month == subquery)
    )


def get_dates_with_high_weighted_value(threshold):
    return select(DimCalendar).where(DimCalendar.weighted_value > threshold)


def get_fiscal_year_to_date():
    today = date.today()
    return select(DimCalendar).where(
        and_(
            DimCalendar.fiscal_year == select(DimCalendar.fiscal_year).where(
                DimCalendar.calendar_date == today).scalar_subquery(),
            DimCalendar.calendar_date <= today
        )
    )
