import pandas as pd
from sqlalchemy import text


def print_hello(name):
    print(f"~~~~~~~~~~~~Hello, {name}!~~~~~~~~~~~~")


def execute_sql(engine, sql: str, params: dict = None, fetch: bool = True, parse_dates: list = None):
    """
    Execute SQL with optional fetching of results.

    Args
    -------
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine instance, representing a database connection.
        sql (str): The SQL query string.
        params (dict, optional): The parameters for the SQL query. Defaults to None.
        fetch (bool, optional): Whether to fetch results. Defaults to True.
        parse_dates (list, optional): List of column names to parse as timestamp. Defaults to None.

    Returns
    -------
        pd.DataFrame or None: The query results if fetch is True, otherwise None.
    """
    try:
        with engine.connect() as con:
            if fetch:
                return pd.read_sql(text(sql), con, params=params, parse_dates=parse_dates)
            else:
                con.execute(text(sql), params)
                con.commit()
    except Exception:
        raise


def pd_append_sql(con, df: pd.DataFrame, name: str, schema: str) -> None:
    """
    TRUNCATE and appends a DataFrame to an existing SQL table.

    Establishes a database connection using the given engine and appends the DataFrame to the specified table
    within the provided schema. If the table does not exist, an error will occur.

    Parameters:
        con: SQLAlchemy Connection object used to interact with the database.
        df (pd.DataFrame): The DataFrame to be appended to the SQL table.
        name (str): The name of the target SQL table.
        schema (str): The name of the schema where the table is located.
    """
    sql = f"""TRUNCATE TABLE {schema}.{name};"""
    con.execute(text(sql))
    print(f'{schema}.{name}:', df.shape)
    df.to_sql(name=name, schema=schema, con=con,
              index=False, if_exists="append")


def website_boolean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the 'website_name' column of the input DataFrame into a binary indicator column named 'is_online'.

    If 'website_name' is 0, it remains 0; otherwise, it is converted to 1.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing a column named 'website_name'.

    Returns:
        pd.DataFrame: A DataFrame with 'is_online' binary column.
    """
    df_ = df.rename(columns={'website_name': 'is_online'})
    df_['is_online'] = df_['is_online'] != '0'

    return df_


def assign_age_group(df: pd.DataFrame) -> pd.DataFrame:
    """
    將 age 欄位轉換為對應的年齡區間標籤，若整欄為空則不進行處理。
    """
    age_labels = ["<16", "16-25", "26-35", "36-45", "46-55", "56-65", ">65"]
    bins = [0, 15, 25, 35, 45, 55, 65, 150]
    if not df['age'].isna().all():
        df['age'] = pd.cut(
            df['age'], bins=bins, labels=age_labels,
            right=True, include_lowest=True
        )
    return df
