from sqlalchemy import insert, select, update, and_, or_, null, distinct, case
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
            # target_alias.get_column(col) == source_alias.get_column(col)
            self.join_on_nulls(source_alias.get_column(col), target_alias.get_column(col))
            for col in natural_key_columns
        ]

        # when comparing natural keys there is a problem is null exists.
        # since they default to either '' if it's a string or 0 if it's a number
        # that defuault causes row collisions if '' or 0 already exists in the dataset.
        # and since the first row of every dimension must be a null, then the comparison
        # needs to be removed and that null row should be added separately.
        primary_keys_are_null = [target_alias.get_column(col) == null() for col in primary_key_columns]
        source_natural_keys_are_not_null = [source_alias.get_column(col) != null() for col in natural_key_columns]

        insert_select = (
            select(*select_clause)
            .select_from(source_alias)
            .join(target_alias, and_(*join_clause), isouter = True, full = False)
            .where(and_(
                # *source_natural_keys_are_not_null,
                or_(*primary_keys_are_null)
            ))
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
        target_alias = aliased(target_table, name = "target")
        source_alias = aliased(source_table, name = "source")
        natural_key_columns = self.get_natural_key_column_names()
        change_columns = self.get_change_column_names()
        reserved_columns = self.reserved_columns_names
        primary_key_columns = self.get_primary_key_column_names()

        ####################
        # INSERT new columns
        ####################
        select_clause = [
            target_alias.get_column(column) if column in primary_key_columns
            else source_alias.get_column(column)
            for column in source_alias.get_all_column_names()
        ]

        equal_cols = [
            # target_alias.get_column(col) == source_alias.get_column(col)
            self.join_on_nulls(source_alias.get_column(col), target_alias.get_column(col))
            for col in natural_key_columns
        ]

        changed_cols = [
            self.coalesce_to_default(target_alias.get_column(col)) !=
            self.coalesce_to_default(source_alias.get_column(col))
            for col in change_columns
            if col not in reserved_columns
        ]

        where_condition = and_(
            target_alias.get_column("current_flag") == 1,
            or_(*changed_cols)
        )

        source_primary_keys = [
            source_alias.get_column(column).label(f"source_{column}")
            for column in primary_key_columns
        ]

        target_primary_keys = [
            target_alias.get_column(column) .label(f"target_{column}")
            for column in primary_key_columns
        ]

        source_columns = [
            source_alias.get_column(column)
            for column in source_alias.get_all_column_names()
            if column not in primary_key_columns
        ]

        select_for_update = (
            select(select_clause)
            .select_from(target_alias)
            .join(source_alias, and_(*equal_cols), isouter = False, full = False)
            .where(where_condition)
            .alias("update_old_rows")
        )

        select_for_insert = (
            select(source_alias.get_all_columns())
            .select_from(target_alias)
            .join(source_alias, and_(*equal_cols), isouter = False, full = False)
            .where(where_condition)
        )

        insert_stmt = (
            insert(target_table)
            .from_select(
                target_table.get_all_columns(),
                select_for_insert
            )
        )

        ########################
        # UPDATE all old columns
        ########################
        # equal_cols = [
        #     # target_alias.get_column(col) == source_alias.get_column(col)
        #     self.join_on_nulls(source_alias.get_column(col), target_table.get_column(col))
        #     for col in natural_key_columns
        # ]
        #
        # natural_keys_not_null = [
        #     and_(target_table.get_column(col) != null(), source_alias.get_column(col) != null())
        #     for col in natural_key_columns
        # ]
        #
        # changed_cols = [
        #     self.coalesce_to_default(target_table.get_column(col)) !=
        #     self.coalesce_to_default(source_alias.get_column(col))
        #     for col in change_columns
        #     if col not in reserved_columns
        # ]
        #
        # where_condition = and_(
        #     *equal_cols,
        #     # *natural_keys_not_null,
        #     target_table.get_column("current_flag") == 1,
        #     or_(*changed_cols)
        # )

        equal_cols = [
            target_table.get_column(col) == select_for_update.c[col]
            # self.join_on_nulls(select_stmt.c[col], target_table.get_column(col))
            for col in primary_key_columns
        ]

        update_stmt = (
            update(target_table)
            .values({
                'current_flag': 0,
                'scd2_end_date': func.dateadd(text('day'), -1, select_for_update.c['scd2_start_date'])
            })
            .where(and_(*equal_cols))
        )

        return [insert_stmt, update_stmt]

    def _generate_soft_delete_statement(self, source_table):
        target_table = self.__class__
        target_alias = aliased(target_table, name = "target")
        source_alias = aliased(source_table, name = "source")
        natural_key_columns = self.get_natural_key_column_names()
        primary_key_columns = self.get_primary_key_column_names()

        join_clause = [
            self.join_on_nulls(source_alias.get_column(col), target_alias.get_column(col))
            for col in natural_key_columns
        ]

        primary_keys_are_null = [target_alias.get_column(col) == null() for col in primary_key_columns]
        target_natural_keys_are_not_null = [target_alias.get_column(col) != null() for col in natural_key_columns]

        cte = (
            select(target_alias.get_columns(primary_key_columns))
            .select_from(target_alias)
            .join(source_alias, and_(*join_clause), isouter = True, full = False)
            .where(and_(
                # *target_natural_keys_are_not_null,
                target_alias.get_column("current_flag") == 1,
                or_(*primary_keys_are_null)
            ))
            .distinct()
            .cte("soft_delete_cte")
        )

        soft_delete_stmt = (
            update(target_table)
            .values(active = 0)
            .where(and_(*[target_table.get_column(col) == cte.c[col] for col in primary_key_columns]))
        )
        return [soft_delete_stmt]
