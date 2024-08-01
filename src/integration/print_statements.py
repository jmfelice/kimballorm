from src.integration.generate_statements import (
    generate_create_table_statement,
    generate_crud_statements,
    generate_drop_statement
)
import sqlfluff

config_path = r"C:\Users\jmfel\PycharmProjects\KimballORM\.sqlfluff"


def print_crud_statements(entity_orm, eng):
    compiled_statements = generate_crud_statements(entity_orm)
    printable_statements = entity_orm.compile_sql(eng, compiled_statements)
    for statement in printable_statements:
        print(sqlfluff.fix(statement, config_path = config_path))


def print_drop_statement(entity_orm, eng):
    drop_statement = generate_drop_statement(entity_orm)
    printable_statements = entity_orm.compile_sql(eng, drop_statement)
    print(sqlfluff.fix(printable_statements, config_path = config_path))


def print_create_statement(entity_orm, eng):
    create_statement = generate_create_table_statement(entity_orm)
    printable_statements = entity_orm.compile_sql(eng, create_statement)
    print(sqlfluff.fix(printable_statements, config_path = config_path))
