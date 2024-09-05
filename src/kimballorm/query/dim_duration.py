from ..orm import DimDuration
from sqlalchemy import select, func, and_, between, desc


def from_dim_duration_get_all():
    return select(DimDuration).order_by(DimDuration.duration_order)


def from_dim_duration_get_duration_order():
    return select(DimDuration.duration_description).order_by(DimDuration.duration_order)
