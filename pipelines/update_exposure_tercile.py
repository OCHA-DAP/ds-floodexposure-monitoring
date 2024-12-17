import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.utils import database

ROLL_WINDOW = 7


def rolling_query(table_name):
    return text(
        f"""
        WITH target_dates AS (
            SELECT
                pcode,
                adm_level,
                valid_date,
                sum as original_sum
            FROM app.{table_name}
            WHERE EXTRACT(MONTH FROM valid_date) = :month
                AND EXTRACT(DAY FROM valid_date) = :day
        ),
        date_ranges AS (
            SELECT
                t.pcode,
                t.adm_level,
                t.valid_date as target_date,
                t.original_sum as sum,
                d.valid_date,
                d.sum as daily_sum
            FROM target_dates t
            JOIN app.{table_name} d
                ON d.pcode = t.pcode
                AND d.valid_date BETWEEN t.valid_date - INTERVAL '{ROLL_WINDOW-1} days' AND t.valid_date
        )
        SELECT
            pcode,
            adm_level,
            target_date as valid_date,
            AVG(daily_sum) as rolling_avg
        FROM date_ranges
        GROUP BY pcode, adm_level, target_date
        ORDER BY pcode, target_date;
        """
    )


def assign_tercile(row, boundaries, id_col="pcode"):
    """
    Assign tercile values based on boundaries.
    Returns:
        -1: Below lower tercile
        0: Between terciles
        1: Above upper tercile
    """
    pcode_bounds = boundaries.loc[row[id_col]]
    value = row["rolling_avg"]
    if value < pcode_bounds["lower_tercile"]:
        return -1
    elif value <= pcode_bounds["upper_tercile"]:
        return 0
    else:
        return 1


def save_df(df, sel_date, engine, output_table, id_col="pcode"):

    if df.empty:
        print(
            f"No data retrieved from database for {target_date.strftime('%Y-%m-%d')}"  # noqa
        )
        sys.exit(0)

    print("Computing terciles...")
    tercile_boundaries = df.groupby(id_col)["rolling_avg"].agg(
        lower_tercile=lambda x: np.percentile(x, 33.33),
        upper_tercile=lambda x: np.percentile(x, 66.67),
    )

    df["tercile"] = df.apply(
        lambda row: assign_tercile(row, tercile_boundaries, id_col), axis=1
    )

    df["valid_date"] = pd.to_datetime(df["valid_date"])
    df_sel = df[df.valid_date == sel_date.strftime("%Y-%m-%d")]

    if len(df_sel) == 0:
        print(f"No data available for {sel_date.strftime('%Y-%m-%d')}")
        sys.exit(0)

    print("Writing to database...")
    df_sel.to_sql(
        output_table,
        schema="app",
        con=engine,
        if_exists="replace",
        chunksize=10000,
        index=False,
    )


if __name__ == "__main__":
    target_date = datetime.today() - timedelta(days=1)
    engine = database.get_engine()

    print(f"Computing terciles as of {target_date.strftime('%Y-%m-%d')}")
    print(f"Using {ROLL_WINDOW}-day rolling average")

    # Get data and calculate rolling averages
    try:
        with engine.connect() as con:
            print("Calculating rolling averages and getting data from db...")
            df_standard = pd.read_sql_query(
                rolling_query("floodscan_exposure"),
                con,
                params={"month": target_date.month, "day": target_date.day},
            )
            df_region = pd.read_sql_query(
                rolling_query("floodscan_exposure_regions"),
                con,
                params={"month": target_date.month, "day": target_date.day},
            )
    except Exception as e:
        print(f"Error querying database: {e}")
        sys.exit(1)

    save_df(df_standard, target_date, engine, "current_tercile")
    save_df(df_region, target_date, engine, "current_tercile_region")

    print("Done!")
