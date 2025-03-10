from io import BytesIO

import numpy as np
import ocha_stratus as stratus
import requests
import rioxarray as rxr

from src.constants import PROJECT_PREFIX, STAGE, WORLDPOP_BASE_URL
from src.utils import blob


def get_blob_name(iso3: str):
    iso3 = iso3.lower()
    return (
        f"{PROJECT_PREFIX}/raw/worldpop/"
        f"{iso3}_ppp_2020_1km_Aggregated_UNadj.tif"
    )


def download_worldpop_to_blob(iso3: str, clobber: bool = False):
    iso3 = iso3.lower()
    blob_name = get_blob_name(iso3)
    if not clobber and blob_name in stratus.list_container_blobs(
        name_starts_with=f"{PROJECT_PREFIX}/raw/worldpop/", stage=STAGE
    ):
        print(f"{blob_name} already exists in blob storage")
        return
    url = WORLDPOP_BASE_URL.format(iso3_upper=iso3.upper(), iso3=iso3)
    response = requests.get(url)
    response.raise_for_status()
    blob.upload_blob_data(blob_name, response.content, stage=STAGE)


def load_worldpop_from_blob(iso3: str):
    iso3 = iso3.lower()
    blob_name = get_blob_name(iso3)
    data = blob.load_blob_data(blob_name, stage=STAGE)
    da = rxr.open_rasterio(BytesIO(data))
    da = da.where(da != da.attrs["_FillValue"]).squeeze(drop=True)
    da.attrs["_FillValue"] = np.nan
    return da
