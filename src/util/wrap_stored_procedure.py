from typing import List, Optional
import sqlfluff


def drop_stored_procedure(schema: str, procedure_name: str) -> str:
    """
    Create a SQL string to drop a stored procedure.

    Args:
        schema (str): The schema where the stored procedure is located.
        procedure_name (str): The name of the stored procedure.

    Returns:
        str: The SQL string for dropping the stored procedure.

    Raises:
        TypeError: If the schema or procedure_name is not a string.
    """
    # Validate inputs
    if not isinstance(schema, str):
        raise TypeError("schema must be a string")
    if not isinstance(procedure_name, str):
        raise TypeError("procedure_name must be a string")

    # Create the SQL string to drop the stored procedure
    result = f"DROP PROCEDURE {schema}.{procedure_name}();"

    return result


def create_stored_procedure(
        schema: str,
        procedure_name: str,
        *statements: str,
        declared_variables: Optional[List[str]] = None,
        parameters: Optional[List[str]] = None
) -> str:
    """
    Create a SQL string for a stored procedure that executes a series of SQL statements.

    Args:
        schema (str): The schema where the stored procedure will be created.
        procedure_name (str): The name of the stored procedure.
        *statements (str): SQL statements to be included in the stored procedure.
        declared_variables (Optional[List[str]]): A list of variable declarations for the procedure.
        parameters (Optional[List[str]]): A list of parameters for the stored procedure.

    Returns:
        str: The SQL string for creating the stored procedure.

    Raises: TypeError: If the schema or procedure_name is not a string, or if declared_variables or parameters is not
    a list.
    """
    # Validate inputs
    if not isinstance(schema, str):
        raise TypeError("schema must be a string")
    if not isinstance(procedure_name, str):
        raise TypeError("procedure_name must be a string")
    if declared_variables and not isinstance(declared_variables, list):
        raise TypeError("declared_variables must be a list of strings")
    if parameters and not isinstance(parameters, list):
        raise TypeError("parameters must be a list of strings")

    # Join the statements with semicolons
    statements_string = '\n' + ';\n\n'.join(statements)

    # Format the declared variables if any
    if declared_variables:
        declared_vars_string = 'DECLARE\n    ' + ';\n    '.join(declared_variables) + ';'
    else:
        declared_vars_string = ''

    # Format the parameters if any
    if parameters:
        parameters_string = ', '.join(parameters)
    else:
        parameters_string = ''

    # Create the full stored procedure SQL
    result = f"""
    CREATE OR REPLACE PROCEDURE {schema}.{procedure_name}({parameters_string})
    AS $$
    {declared_vars_string}
    BEGIN
    {statements_string};
    END;
    $$ LANGUAGE plpgsql;
    """

    return sqlfluff.fix(result)
