from sqlalchemy import select
from ..orm import BridgeCategory
from sqlalchemy import or_


def from_bridge_category_get_all():
    return select(BridgeCategory)


def from_bridge_category_get_category(category):
    return (
        select(BridgeCategory)
        .where(or_(BridgeCategory.category.in_(category), BridgeCategory.child_category.in_(category)))
    )


def from_from_bridge_category_get_level(level):
    return select(BridgeCategory).where(BridgeCategory.level == level)


def from_bridge_category_get_leaf_categories():
    return select(BridgeCategory).where(BridgeCategory.isleaf == 1)


def from_bridge_category_get_category_class(category_class):
    return select(BridgeCategory).where(BridgeCategory.category_class == category_class)
