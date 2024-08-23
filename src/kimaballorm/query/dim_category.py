from sqlalchemy import select, or_
from ..orm import DimCategory


def from_dim_category_get_by_category(category):
    return select(DimCategory).where(or_(DimCategory.category.in_(category), DimCategory.child_category.in_(category)))


def from_dim_category_get_category_class(category_class):
    return select(DimCategory).where(DimCategory.category_class.in_(category_class))


def from_dim_category_get_level(level):
    return select(DimCategory).where(DimCategory.level.in_(level))


def from_dim_category_get_active_categories():
    return select(DimCategory).where(DimCategory.active == 1)


def from_dim_category_get_leaf_categories():
    return select(DimCategory).where(DimCategory.isleaf == 1)


def from_dim_category_get_category_like(partial_name):
    return select(DimCategory).where(DimCategory.category.like(f"%{partial_name}%"))
