import os

from dotenv import load_dotenv

load_dotenv()

STAGE = os.getenv("STAGE")

ISO3S = [
    "ner",
    "nga",
    "cmr",
    "tcd",
    "bfa",
    "eth",
    "som",
    "ssd",
    "mli",
    "cod",
    "moz",
    "mwi",
]
CHD_GREEN = "#1bb580"

PROJECT_PREFIX = "ds-floodexposure-monitoring"
FLOODSCAN_COG_FILEPATH = "floodscan/daily/v5/processed"
FIELDMAPS_BASE_URL = "https://data.fieldmaps.io/cod/originals/{iso3}.shp.zip"

WORLDPOP_BASE_URL = (
    "https://data.worldpop.org/GIS/Population/"
    "Global_2000_2020_1km_UNadj/2020/{iso3_upper}/"
    "{iso3}_ppp_2020_1km_Aggregated_UNadj.tif"
)

# specific pcodes for building regions
NORDKIVU1 = "CD61"
SUDKIVU1 = "CD62"
TANGANYIKA1 = "CD74"
BASUELE1 = "CD52"
HAUTUELE1 = "CD53"
TSHOPO1 = "CD51"

REGIONS = [
    {
        "adm_level": 1,
        "iso3": "cod",
        "region_number": 1,
        "region_name": "Zone 1",
        "pcodes": [BASUELE1, HAUTUELE1, TSHOPO1],
    },
    {
        "adm_level": 1,
        "iso3": "cod",
        "region_number": 2,
        "region_name": "Zone 2",
        "pcodes": [NORDKIVU1, SUDKIVU1],
    },
    {
        "adm_level": 1,
        "iso3": "cod",
        "region_number": 3,
        "region_name": "Zone 3",
        "pcodes": [TANGANYIKA1],
    },
]
