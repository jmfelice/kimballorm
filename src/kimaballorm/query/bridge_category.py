from sqlalchemy import select
from ..orm import BridgeCategory


def get_all_bridge_categories():
    return select(BridgeCategory)


def get_bridge_categories_by_category_key(category_key):
    return select(BridgeCategory).where(BridgeCategory.category_key == category_key)


def get_bridge_categories_by_child_category_key(child_category_key):
    return select(BridgeCategory).where(BridgeCategory.child_category_key == child_category_key)


def get_bridge_categories_by_category(category):
    return select(BridgeCategory).where(BridgeCategory.category == category)


def get_bridge_categories_by_child_category(child_category):
    return select(BridgeCategory).where(BridgeCategory.child_category == child_category)


def get_bridge_categories_by_level(level):
    return select(BridgeCategory).where(BridgeCategory.level == level)


def get_leaf_bridge_categories():
    return select(BridgeCategory).where(BridgeCategory.isleaf == 1)


def get_bridge_categories_by_category_class(category_class):
    return select(BridgeCategory).where(BridgeCategory.category_class == category_class)


def get_bridge_categories_by_order_range(min_order, max_order):
    return select(BridgeCategory).where(
        BridgeCategory.category_order.between(min_order, max_order)
    )


def get_bridge_category_hierarchy(category):
    return (
        select(BridgeCategory)
        .where(
            (BridgeCategory.category == category) |
            (BridgeCategory.child_category == category)
        ).order_by(BridgeCategory.level))


def get_bridge_categories_ordered():
    return select(BridgeCategory).order_by(BridgeCategory.category_order)
