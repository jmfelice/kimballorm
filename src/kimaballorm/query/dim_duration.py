from ..orm import DimDuration
from sqlalchemy import select, func, and_, between, desc


def get_dim_duration():
    return select(DimDuration).order_by(DimDuration.duration_order)


def get_duration_order():
    return select(DimDuration.duration_description).order_by(DimDuration.duration_order)
