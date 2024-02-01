def format_sql_query(query:str) -> str:
    """
    Objective: this function prepares a query by transforming the column names into a specific format. 
    It is ideally applied when querying variables within structured JSON data

    Args:
        query: the query that needs to be transformed.

    Input example:
        SELECT 
            ColumnName1.Variable1,
            ColumnName1.Variable2,
            ColumnName2.Variable1
        FROM your_table

    Output example:
        SELECT 
            ColumnName1.Variable1 as columnname1_variable1,
            ColumnName1.Variable2 as columnname1_variable2,
            ColumnName2.Variable1 as columnname2_variable1
        FROM your_table

    """
    sql_query = query.strip().replace(',', '').split('\n')
    cols = [data +' as '+ data.strip().replace('.', '_').lower() for data in sql_query if data.strip().startswith(('select', 'from')) != True]
    result = sql_query[0] + '\n' + ',\n'.join(cols) + '\n' + sql_query[-1]
    return result