import ocha_stratus as stratus
import pandas as pd
import requests

from src.constants import FIELDMAPS_BASE_URL, PROJECT_PREFIX, STAGE
from src.utils import blob


def load_geo_data(iso3s, regions, save_to_database=True):
    """Load geo data from blob storage and save a lookup table to database."""
    adms = []
    for iso3 in iso3s:
        print(f"loading {iso3} adm to migrate")
        gdf_in = load_codab_from_blob(iso3, admin_level=2)
        adms.append(gdf_in)
    adm = pd.concat(adms, ignore_index=True)

    for adm_level in range(3):
        adm[f"ADM{adm_level}_NAME"] = (
            adm[f"ADM{adm_level}_FR"]
            .fillna(adm[f"ADM{adm_level}_EN"])
            .fillna(adm[f"ADM{adm_level}_PT"])
        )
    adm.drop(columns=["geometry"], inplace=True)
    adm.columns = adm.columns.str.lower()

    region_dicts = []
    for region in regions:
        adm_names = adm[
            adm[f"adm{region['adm_level']}_pcode"].isin(region["pcodes"])
        ][f"adm{region['adm_level']}_name"].unique()
        region_dicts.append(
            {
                "admregion_pcode": f'{region["iso3"]}_region_{region["region_number"]}',  # noqa
                "admregion_name": f'{region["region_name"]} ({", ".join(adm_names)})',  # noqa
            }
        )

    df_out = pd.concat([adm, pd.DataFrame(region_dicts)], ignore_index=True)

    if save_to_database:
        df_out.to_sql(
            "admin_lookup",
            schema="app",
            con=stratus.get_engine(STAGE, write=True),
            if_exists="replace",
            index=False,
        )


def get_blob_name(iso3: str):
    iso3 = iso3.lower()
    return f"{PROJECT_PREFIX}/raw/codab/{iso3}.shp.zip"


def download_codab_to_blob(iso3: str, clobber: bool = False):
    iso3 = iso3.lower()
    blob_name = get_blob_name(iso3)
    if not clobber and blob_name in stratus.list_container_blobs(
        name_starts_with=f"{PROJECT_PREFIX}/raw/codab/", stage=STAGE
    ):
        print(f"{blob_name} already exists in blob storage")
        return
    url = FIELDMAPS_BASE_URL.format(iso3=iso3)
    response = requests.get(url)
    response.raise_for_status()

    # Should eventually get this from ocha-stratus
    blob.upload_blob_data(blob_name, response.content, stage=STAGE)


def load_codab_from_blob(iso3: str, admin_level: int = 0):
    iso3 = iso3.lower()
    shapefile = f"{iso3}_adm{admin_level}.shp"
    gdf = stratus.load_shp_from_blob(
        blob_name=get_blob_name(iso3), shapefile=shapefile, stage=STAGE
    )
    return gdf
