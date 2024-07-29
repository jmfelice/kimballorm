from sqlalchemy import select, delete, update, and_, or_, null, insert
from sqlalchemy.orm import aliased
from .mixin_utility import UtilityBase


class SyncFact(UtilityBase):
    """
    Notes:
    1. Fact tables join on the Unique Columns.  That is the list of columns that make the row unique.
    Notable this excludes the primary key which gets rebuilt in the source table and, therefore, could cause
    confusion.
    """
    def sync_with_source(self, source_table):
        statements = []
        statements.extend(self._generate_insert_statement(source_table))
        statements.extend(self._generate_update_statement(source_table))
        statements.extend(self._generate_delete_statement(source_table))
        return statements

    def _generate_insert_statement(self, source_table):
        target_table = self.__class__
        target_alias = aliased(target_table, name="target")
        source_alias = aliased(source_table, name="source")
        unique_columns = self.get_unique_column_names()
        primary_key_columns = self.get_primary_key_column_names()

        join_clause = [
            self.coalesce_to_default(target_alias.get_column(col)) ==
            self.coalesce_to_default(source_alias.get_column(col))
            for col in unique_columns
        ]

        where_clause = [
            target_alias.get_column(col) == null()
            for col in primary_key_columns
        ]

        insert_select = (
            select(source_alias)
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
        unique_columns = self.get_unique_column_names()
        change_columns = self.get_change_column_names()

        equal_cols = [
            self.coalesce_to_default(target_table.get_column(col)) ==
            self.coalesce_to_default(source_alias.get_column(col))
            for col in unique_columns
        ]

        changed_cols = [
            self.coalesce_to_default(target_table.get_column(col)) !=
            self.coalesce_to_default(source_alias.get_column(col))
            for col in change_columns
        ]

        where_condition = and_(*equal_cols, or_(*changed_cols))

        update_stmt = (
            update(target_table)
            .values({col: source_alias.get_column(col) for col in change_columns})
            .where(where_condition)
        )
        return [update_stmt]

    def _generate_delete_statement(self, source_table):
        target_table = self.__class__
        target_alias = aliased(target_table, name="target")
        source_alias = aliased(source_table, name="source")
        unique_columns = self.get_unique_column_names()
        primary_key_columns = self.get_primary_key_column_names()

        join_clause = [
            self.coalesce_to_default(target_alias.get_column(col)) ==
            self.coalesce_to_default(source_alias.get_column(col))
            for col in unique_columns
        ]

        where_clause = [
            source_alias.get_column(col) == null()
            for col in primary_key_columns
        ]

        cte = (
            select(target_alias.get_columns(primary_key_columns))
            .select_from(target_alias)
            .join(source_alias, and_(*join_clause), isouter = True, full = True)
            .where(and_(*where_clause))
            .cte("soft_delete_cte")
        )

        delete_stmt = (
            delete(target_table)
            .where(and_(*[target_table.get_column(col) == cte.c[col] for col in primary_key_columns]))
        )
        return [delete_stmt]
