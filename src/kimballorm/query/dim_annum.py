from ..orm import DimAnnum
from sqlalchemy import select, func, and_, between, desc


def from_dim_annum_get_all():
    return select(DimAnnum).order_by(DimAnnum.annum_order)


def from_dim_annum_get_order():
    return select(DimAnnum.annum_description).order_by(DimAnnum.annum_order)
