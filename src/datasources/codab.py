import ocha_stratus as ocha
import requests

from src.constants import FIELDMAPS_BASE_URL, PROJECT_PREFIX, STAGE
from src.utils import blob


def get_blob_name(iso3: str):
    iso3 = iso3.lower()
    return f"{PROJECT_PREFIX}/raw/codab/{iso3}.shp.zip"


def download_codab_to_blob(iso3: str, clobber: bool = False):
    iso3 = iso3.lower()
    blob_name = get_blob_name(iso3)
    if not clobber and blob_name in ocha.list_container_blobs(
        name_starts_with=f"{PROJECT_PREFIX}/raw/codab/", stage=STAGE
    ):
        print(f"{blob_name} already exists in blob storage")
        return
    url = FIELDMAPS_BASE_URL.format(iso3=iso3)
    response = requests.get(url)
    response.raise_for_status()

    # Should eventually get this from ocha-stratus
    blob._upload_blob_data(blob_name, response.content, stage=STAGE)


def load_codab_from_blob(iso3: str, admin_level: int = 0):
    iso3 = iso3.lower()
    shapefile = f"{iso3}_adm{admin_level}.shp"
    gdf = ocha.load_shp_from_blob(
        blob_name=get_blob_name(iso3), shapefile=shapefile, stage=STAGE
    )
    return gdf
