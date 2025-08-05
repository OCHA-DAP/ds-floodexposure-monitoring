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
