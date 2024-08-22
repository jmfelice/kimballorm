from ..orm import DimAnnum
from sqlalchemy import select, func, and_, between, desc


def get_dim_annum():
    return select(DimAnnum).order_by(DimAnnum.annum_order)


def get_annum_order():
    return select(DimAnnum.annum_description).order_by(DimAnnum.annum_order)
