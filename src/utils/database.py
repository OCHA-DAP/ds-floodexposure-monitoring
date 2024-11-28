from sqlalchemy import (
    CHAR,
    REAL,
    Column,
    Date,
    Integer,
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
        Column("adm0_pcode", String),
        Column("adm1_pcode", String),
        Column("adm2_pcode", String),
        Column("date", Date),
        Column("eff_date", Date),
        Column("dayofyear", Integer),
        Column("total_exposed", REAL),
        Column("roll7", REAL),
    ]

    unique_constraint_columns = ["adm2_pcode", "date"]

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
    https://stackoverflow.com/questions/55187884/insert-into-postgresql-table-from-pandas-with-on-conflict-update

    Parameters
    ----------
    table : sqlalchemy.sql.schema.Table
        The SQLAlchemy Table object where the data will be inserted or updated.
    conn : sqlalchemy.engine.Connection
        The SQLAlchemy connection object used to execute the upsert operation.
    keys : list of str
        The list of column names used as keys for the upsert operation.
    data_iter : iterable
        An iterable of tuples or lists containing the data to be inserted or updated.
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
