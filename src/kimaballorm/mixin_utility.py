from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint, ForeignKeyConstraint, func, inspect
from sqlalchemy import or_, and_, null
from sqlalchemy.orm.util import AliasedClass
from typing import List
from .default_data_types import DEFAULT_VALUES


class UtilityBase:
    @classmethod
    def get_mapped_class(cls):
        if isinstance(cls, AliasedClass):
            return cls._aliased_insp.mapper.class_
        return cls

    @classmethod
    def get_column_info(cls, column):
        """
        Retrieve the underlying SQLAlchemy Column object for a given column.

        This method normalizes access to column information, handling both
        regular columns and aliased columns. For aliased columns, it resolves
        to the original column from the mapper, ensuring that the returned
        object is suitable for further SQLAlchemy operations.

        Parameters:
        column (str or InstrumentedAttribute): The name of the column as a
        string or an InstrumentedAttribute representing the column.

        Returns:
        Column: The SQLAlchemy Column object corresponding to the provided
        column name or InstrumentedAttribute.

        Usage:
        - Use this method when you need to perform operations that require
          the original column object, such as type introspection or complex
          query building.
        - This method is particularly useful when working with aliased
          classes, as it ensures that you get the correct underlying column
          regardless of whether the class is aliased or not.

        Example:
        ```
        # For a regular column
        column_info = MyClass.get_column_info('column_name')

        # For an aliased column
        aliased_table = MyClass.create_alias('my_alias')
        aliased_column_info = aliased_table.get_column_info(aliased_table.some_column)
        ```
        """
        if isinstance(column, str):
            column = cls.get_column(column)

        if isinstance(cls, AliasedClass):
            # For aliased classes, we need to get the original mapper
            mapper = inspect(cls).mapper
            if hasattr(column, 'key'):
                # Get the original column from the mapper
                return mapper.columns[column.key]

        # For regular columns or already processed aliased columns
        return column

    @classmethod
    def get_table(cls):
        mapped_class = cls.get_mapped_class()
        return mapped_class.__table__

    @classmethod
    def get_table_name(cls):
        return cls.__tablename__

    @classmethod
    def get_schema_name(cls):
        return cls.__table__.schema

    @classmethod
    def get_table_attribute(cls, attr):
        table = cls.get_table()
        return getattr(table, attr)

    @classmethod
    def get_column(cls, column_name: str):
        if isinstance(cls, AliasedClass):
            return getattr(cls, column_name)
        return cls.get_table().c[column_name]

    @classmethod
    def get_columns(cls, column_names: List[str]):
        return [cls.get_column(column_name) for column_name in column_names]

    @classmethod
    def get_all_columns(cls):
        if isinstance(cls, AliasedClass):
            return [getattr(cls, c.name) for c in cls._aliased_insp.mapper.columns]
        return list(cls.get_table().c)

    @classmethod
    def get_all_column_names(cls):
        if isinstance(cls, AliasedClass):
            return [c.name for c in cls._aliased_insp.mapper.columns]
        return [column.name for column in cls.get_table().c]

    @classmethod
    def get_table_arguments(cls):
        mapped_class = cls.get_mapped_class()
        return mapped_class.__table_args__

    @classmethod
    def get_constraint_columns(cls):
        return cls.get_table_attribute("constraints")

    @classmethod
    def get_foreign_key_column_names(cls) -> List[str]:
        all_constraints = cls.get_constraint_columns()
        rslt = [fk.columns for fk in all_constraints if isinstance(fk, ForeignKeyConstraint)]
        return [col.name for fk in rslt for col in fk]

    @classmethod
    def get_unique_column_names(cls) -> List[str]:
        all_constraints = cls.get_constraint_columns()
        rslt = [uc.columns for uc in all_constraints if isinstance(uc, UniqueConstraint)]
        return [col.name for uc in rslt for col in uc]

    @classmethod
    def get_primary_key_column_names(cls) -> List[str]:
        all_constraints = cls.get_constraint_columns()
        rslt = [pk.columns for pk in all_constraints if isinstance(pk, PrimaryKeyConstraint)]
        return [col.name for pk in rslt for col in pk]

    @classmethod
    def get_natural_key_column_names(cls) -> List[str]:
        key = cls.get_custom_attribute("natural_key")
        if isinstance(key, str):
            key = [key]
        return key

    @classmethod
    def get_change_column_names(cls) -> List[str]:
        all_column_names = cls.get_all_column_names()
        unique_column_names = cls.get_unique_column_names()
        primary_key_column_names = cls.get_primary_key_column_names()
        natural_key_column_names = cls.get_natural_key_column_names()

        rslt = [
            name for name in all_column_names if
            name not in (unique_column_names or []) and
            name not in (primary_key_column_names or []) and
            name not in (natural_key_column_names or [])
        ]

        return rslt

    @classmethod
    def get_custom_attribute(cls, attr: str):
        mapped_class = cls.get_mapped_class()
        all_custom_info = mapped_class.__custom_info__
        all_custom_attributes = next((arg for arg in all_custom_info if isinstance(arg, dict)))
        return all_custom_attributes.get(attr)

    @classmethod
    def get_table_type(cls):
        return cls.get_custom_attribute("table_type")

    @classmethod
    def get_column_type(cls, column):
        column_info = cls.get_column_info(column)
        return column_info.type.__class__

    @classmethod
    def get_default_column_value(cls, column):
        column_info = cls.get_column_info(column)
        column_type = column_info.type.__class__
        return DEFAULT_VALUES[column_type]

    @classmethod
    def coalesce_to_default(cls, column):
        column_info = cls.get_column_info(column)
        default_column_value = cls.get_default_column_value(column_info)
        return func.coalesce(column_info, default_column_value)

    @classmethod
    def create_alias(cls, alias_name):
        from sqlalchemy.orm import aliased
        return aliased(cls, name = alias_name)

    @classmethod
    def get_sp_update_source_table(cls):
        table_name = cls.get_table_name()
        return f"finance_etl.sp_update_source_table_{table_name}"

    @classmethod
    def get_source_entity(cls):
        return None

    @classmethod
    def get_source_table_name(cls):
        source_entity = cls.get_source_entity()
        if source_entity is None:
            return ''
        source_schema_name = source_entity().get_schema_name()
        source_table_name = source_entity().get_table_name()
        return f"{source_schema_name}.{source_table_name}"

    @classmethod
    def join_on_nulls(cls, source_column, target_column):
        join_condition = or_(
            source_column == target_column,
            and_(source_column == null(), target_column == null()).self_group()
        ).self_group()
        return join_condition

    def compile_sql(self, statements, engine = None):
        compiled_statements = []
        for stmt in statements:
            if engine:
                compiled_stmt = stmt.compile(
                    engine,
                    compile_kwargs = {"literal_binds": True}
                )
            else:
                compiled_stmt = stmt.compile(
                    compile_kwargs = {"literal_binds": True}
                )
            compiled_statements.append(str(compiled_stmt))
        return compiled_statements


