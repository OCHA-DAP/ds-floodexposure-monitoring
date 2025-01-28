import os
from typing import List, Literal

import pandas as pd
from dotenv import load_dotenv
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
    create_engine,
)
from sqlalchemy.dialects.postgresql import insert

load_dotenv()

AZURE_DB_PW_DEV = os.getenv("AZURE_DB_PW_DEV")
AZURE_DB_PW_PROD = os.getenv("AZURE_DB_PW_PROD")
AZURE_DB_UID = os.getenv("AZURE_DB_UID")
AZURE_DB_BASE_URL = "postgresql+psycopg2://{uid}:{pw}@{db_name}.postgres.database.azure.com/postgres"  # noqa: E501
STAGE = os.getenv("STAGE")


def get_engine(stage: Literal["dev", "prod"] = STAGE):
    if stage == "dev":
        url = AZURE_DB_BASE_URL.format(
            uid=AZURE_DB_UID, pw=AZURE_DB_PW_DEV, db_name="chd-rasterstats-dev"
        )
    elif stage == "prod":
        url = AZURE_DB_BASE_URL.format(
            uid=AZURE_DB_UID,
            pw=AZURE_DB_PW_PROD,
            db_name="chd-rasterstats-prod",
        )
    else:
        raise ValueError(f"Invalid stage: {stage}")
    return create_engine(url)


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


def get_existing_stats_dates(iso3: str, engine) -> list:
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
    query = f"""
    SELECT *
    FROM app.floodscan_exposure
    WHERE pcode IN ({", ".join([f"'{pcode}'" for pcode in pcodes])})
    """
    return pd.read_sql(query, con=engine)
