import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.constants import ISO3S
from src.datasources import codab, floodscan
from src.utils import blob

load_dotenv()

AZURE_DB_PW_DEV = os.getenv("AZURE_DB_PW_DEV")
AZURE_DB_UID = os.getenv("AZURE_DB_UID")
LOCAL_DATA_DIR = Path("pipelines/data")


def migrate_data_to_repo(clobber: bool = False):
    save_path = LOCAL_DATA_DIR / "adm.shp"
    if save_path.exists() and not clobber:
        return
    adms = []
    for iso3 in ISO3S:
        print(f"loading {iso3} adm to migrate")
        gdf_in = codab.load_codab_from_blob(iso3, admin_level=2)
        adms.append(gdf_in)
    adm = pd.concat(adms, ignore_index=True)
    adm.to_file(LOCAL_DATA_DIR / "adm.shp")


def load_data(engine):
    print("Loading data...")
    migrate_data_to_repo()
    adm = gpd.read_file(LOCAL_DATA_DIR / "adm.shp")
    for adm_level in range(3):
        adm[f"ADM{adm_level}_NAME"] = adm[f"ADM{adm_level}_EN"].fillna(
            adm[f"ADM{adm_level}_FR"]
        )
    adm.drop(columns=["geometry"], inplace=True)
    adm.columns = adm.columns.str.lower()
    adm.to_sql(
        "adm",
        schema="app",
        con=engine,
        if_exists="replace",
        index=False,
    )

    def calculate_rolling(group, window=7):
        group[f"roll{window}"] = (
            group["total_exposed"].rolling(window=window).mean()
        )
        return group

    window = 7
    for iso3 in ISO3S:
        print(f"Getting data for {iso3}")
        blob_name = floodscan.get_blob_name(iso3, data_type="exposure_tabular")
        df = blob.load_parquet_from_blob(blob_name)
        df.columns = df.columns.str.lower()
        df = df.merge(adm[["adm1_pcode", "adm2_pcode", "adm0_pcode"]])
        df = df.sort_values("date")
        df = (
            df.groupby("adm2_pcode")
            .apply(calculate_rolling, window=window, include_groups=False)
            .reset_index(level=0)
        )
        df["dayofyear"] = df["date"].dt.dayofyear
        df["eff_date"] = pd.to_datetime(df["dayofyear"], format="%j")
        df["iso3"] = iso3
        df = df[
            [
                "iso3",
                "adm0_pcode",
                "adm1_pcode",
                "adm2_pcode",
                "date",
                "eff_date",
                "dayofyear",
                "total_exposed",
                "roll7",
            ]
        ]
        print(f"Dataframe is {len(df)} rows...")
        df.to_sql(
            "flood_exposure",
            schema="app",
            con=engine,
            if_exists="append",
            chunksize=100000,
            index=False,
        )


if __name__ == "__main__":
    print("Populating database...")
    engine = create_engine(
        f"postgresql+psycopg2://{AZURE_DB_UID}:{AZURE_DB_PW_DEV}@chd-rasterstats-dev.postgres.database.azure.com/postgres"  # noqa: E501
    )
    data = load_data(engine)
    print("Database update completed.")
