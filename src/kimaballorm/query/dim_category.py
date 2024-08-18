from sqlalchemy import select
from ..orm import DimCategory


def get_dim_category_by_category_key(category_key):
    return select(DimCategory).where(DimCategory.category_key == category_key)


def get_dim_category_by_category(category):
    return select(DimCategory).where(DimCategory.category == category)


def get_dim_categories_by_parent_category(parent_category):
    return select(DimCategory).where(DimCategory.parent_category == parent_category)


def get_dim_categories_by_category_class(category_class):
    return select(DimCategory).where(DimCategory.category_class == category_class)


def get_dim_categories_by_level(level):
    return select(DimCategory).where(DimCategory.level == level)


def get_active_dim_categories():
    return select(DimCategory).where(DimCategory.active == 1)


def get_leaf_dim_categories():
    return select(DimCategory).where(DimCategory.isleaf == 1)


def get_dim_categories_by_category_order_range(start_order, end_order):
    return select(DimCategory).where(DimCategory.category_order.between(start_order, end_order))


def get_dim_categories_by_multiple_categories(categories):
    return select(DimCategory).where(DimCategory.category.in_(categories))


def get_dim_categories_by_partial_category_name(partial_name):
    return select(DimCategory).where(DimCategory.category.like(f"%{partial_name}%"))
