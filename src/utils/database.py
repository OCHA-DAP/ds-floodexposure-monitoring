from typing import List

import pandas as pd
from sqlalchemy import (
    CHAR,
    REAL,
    TEXT,
    Column,
    Date,
    MetaData,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import insert


def create_flood_exposure_table(dataset, engine):
    """
    Create a table for storing flood exposure data in the database.

    Parameters
    ----------
    dataset : str
        The name of the dataset for which the table is being created.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy engine object used to connect to the database.

    Returns
    -------
    None
    """

    metadata = MetaData()
    columns = [
        Column("iso3", CHAR(3)),
        Column("adm_level", TEXT),
        Column("valid_date", Date),
        Column("pcode", String),
        Column("sum", REAL),
    ]

    unique_constraint_columns = ["pcode", "valid_date"]

    Table(
        f"{dataset}",
        metadata,
        *columns,
        UniqueConstraint(
            *unique_constraint_columns,
            name=f"{dataset}_unique",
            postgresql_nulls_not_distinct=True,
        ),
        schema="app",
    )

    metadata.create_all(engine)
    return


def get_existing_stats_dates(iso3: str, engine) -> list:
    """
    Retrieve list of dates for which flood statistics exist
    for a given country.

    Parameters
    ----------
    iso3 : str
        Three-letter ISO country code
    engine : Engine
        SQLAlchemy database engine

    Returns
    -------
    list
        Dates with existing flood statistics
    """
    query = f"""
    SELECT DISTINCT valid_date
    FROM app.floodscan_exposure
    WHERE iso3 = '{iso3.upper()}'
    ORDER BY valid_date
    """
    df_unique_dates = pd.read_sql(query, con=engine)
    df_unique_dates["valid_date"] = pd.to_datetime(
        df_unique_dates["valid_date"]
    )
    return df_unique_dates["valid_date"].to_list()


def get_existing_adm_stats(pcodes: List[str], engine) -> pd.DataFrame:
    """
    Fetch flood exposure statistics for specified administrative regions.

    Parameters
    ----------
    pcodes : List[str]
        List of administrative region codes
    engine : Engine
        SQLAlchemy database engine

    Returns
    -------
    pd.DataFrame
        Flood exposure statistics for requested regions
    """
    query = f"""
    SELECT *
    FROM app.floodscan_exposure
    WHERE pcode IN ({", ".join([f"'{pcode}'" for pcode in pcodes])})
    """
    return pd.read_sql(query, con=engine)


def postgres_upsert(table, conn, keys, data_iter, constraint=None):
    """
    Perform an upsert (insert or update) operation on a PostgreSQL table. Adapted from:
    https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update # noqa: E501

    Parameters
    ----------
    table : sqlalchemy.sql.schema.Table
        The SQLAlchemy Table object where the data will be inserted or updated.
    conn : sqlalchemy.engine.Connection
        The SQLAlchemy connection object used to execute the upsert operation.
    keys : list of str
        The list of column names used as keys for the upsert operation.
    data_iter : iterable
        An iterable of tuples or lists containing the data to be inserted or
        updated.
    constraint_name : str
        Name of the uniqueness constraint

    Returns
    -------
    None
    """
    if not constraint:
        constraint = f"{table.table.name}_unique"
    data = [dict(zip(keys, row)) for row in data_iter]
    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=constraint,
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
    return
