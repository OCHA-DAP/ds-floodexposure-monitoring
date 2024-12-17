from typing import List

import pandas as pd

from src.constants import REGIONS
from src.utils import database


def get_existing_adm_stats(pcodes: List[str], engine) -> pd.DataFrame:
    query = f"""
    SELECT *
    FROM app.floodscan_exposure
    WHERE pcode IN ({", ".join([f"'{pcode}'" for pcode in pcodes])})
    """
    return pd.read_sql(query, con=engine)


if __name__ == "__main__":
    engine = database.get_engine(stage="dev")

    for region in REGIONS:
        print(f"Processing {region['iso3']} region {region['region_number']}")
        adm_stats_df = get_existing_adm_stats(region["pcodes"], engine)
        region_stats_df = (
            adm_stats_df.groupby("valid_date")["sum"].sum().reset_index()
        )
        region_stats_df["iso3"] = region["iso3"].upper()
        region_stats_df["pcode"] = (
            f'{region["iso3"]}_region_{region["region_number"]}'
        )
        region_stats_df["adm_level"] = "region"
        region_stats_df.to_sql(
            "floodscan_exposure_regions",
            schema="app",
            con=engine,
            if_exists="replace",
            chunksize=10000,
            index=False,
        )
