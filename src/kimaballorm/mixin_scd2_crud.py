from sqlalchemy import insert, select, update, and_, or_, null
from sqlalchemy import literal, func
from sqlalchemy.sql import text
from sqlalchemy.orm import aliased
from .mixin_utility import UtilityBase


class SyncSCD2(UtilityBase):
    """
    Notes:
    1. Dimensions get joined on the business key or 'natural key'.  That is the column that defines the purpose
    of the table.  For SCD of type 2 this is different from the unique key.  As a result it is imperitive that the
    natural key be idenified for the table when the class is instantiated.
    """
    reserved_columns_names = ["scd2_start_date", "scd2_end_date", "current_flag"]

    def sync_with_source(self, source_table):
        statements = []
        statements.extend(self._generate_update_statement(source_table))
        statements.extend(self._generate_insert_statement(source_table))
        statements.extend(self._generate_soft_delete_statement(source_table))
        return statements

    def _generate_insert_statement(self, source_table):
        target_table = self.__class__
        target_alias = aliased(target_table, name = "target")
        source_alias = aliased(source_table, name = "source")
        natural_key_columns = self.get_natural_key_column_names()
        primary_key_columns = self.get_primary_key_column_names()

        select_clause = [
            literal('1900-01-01').label('scd2_start_date') if column == 'scd2_start_date'
            else source_alias.get_column(column)
            for column in target_table.get_all_column_names()
        ]

        join_clause = [
            self.coalesce_to_default(target_alias.get_column(col)) ==
            self.coalesce_to_default(source_alias.get_column(col))
            for col in natural_key_columns
        ]

        where_clause = [
            target_alias.get_column(col) == null()
            for col in primary_key_columns
        ]

        insert_select = (
            select(*select_clause)
            .select_from(source_alias)
            .join(target_alias, and_(*join_clause), isouter=True, full=True)
            .where(and_(*where_clause))
        )

        insert_stmt = (
            insert(target_table)
            .from_select(
                target_table.get_all_columns(),
                insert_select)
            )
        return [insert_stmt]

    def _generate_update_statement(self, source_table):
        target_table = self.__class__
        source_alias = aliased(source_table, name="source")
        natural_key_columns = self.get_natural_key_column_names()
        change_columns = self.get_change_column_names()
        reserved_columns = self.reserved_columns_names

        equal_cols = [
            self.coalesce_to_default(target_table.get_column(col)) ==
            self.coalesce_to_default(source_alias.get_column(col))
            for col in natural_key_columns
        ]

        changed_cols = [
            self.coalesce_to_default(target_table.get_column(col)) !=
            self.coalesce_to_default(source_alias.get_column(col))
            for col in change_columns
            if col not in reserved_columns
        ]

        where_condition = and_(*equal_cols, target_table.get_column("current_flag") == 1, or_(*changed_cols))

        update_stmt = (
            update(target_table)
            .values({
                'current_flag' : 0,
                'scd2_end_date': func.date_sub(getattr(source_alias, 'scd2_start_date'), text('1 day'))
            })
            .where(where_condition)
        )

        insert_stmt = (
            insert(target_table)
            .from_select(
                target_table.get_all_columns(),
                select(source_alias).where(where_condition)
            )
        )
        return [update_stmt, insert_stmt]

    def _generate_soft_delete_statement(self, source_table):
        target_table = self.__class__
        target_alias = aliased(target_table, name="target")
        source_alias = aliased(source_table, name="source")
        natural_key_columns = self.get_natural_key_column_names()
        primary_key_columns = self.get_primary_key_column_names()

        join_clause = [
            self.coalesce_to_default(target_alias.get_column(col)) ==
            self.coalesce_to_default(source_alias.get_column(col))
            for col in natural_key_columns
        ]

        where_clause = [
            source_alias.get_column(col) == null()
            for col in primary_key_columns
        ]

        cte = (
            select(target_alias.get_columns(primary_key_columns))
            .select_from(target_alias)
            .join(source_alias, and_(*join_clause), isouter = True, full = True)
            .where(and_(*where_clause, target_alias.get_column("active") == 1))
            .cte("soft_delete_cte")
        )

        soft_delete_stmt = (
            update(target_table)
            .values(current_flag=0)
            .where(and_(*[target_table.get_column(col) == cte.c[col] for col in primary_key_columns]))
        )
        return [soft_delete_stmt]
