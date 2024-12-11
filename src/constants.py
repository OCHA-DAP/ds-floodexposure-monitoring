ISO3S = ["ner", "nga", "cmr", "tcd", "bfa", "eth", "som", "ssd", "mli", "cod"]
CHD_GREEN = "#1bb580"

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
        "region_name": "Zone 1",
        "pcodes": [BASUELE1, HAUTUELE1, TSHOPO1],
    },
    {
        "adm_level": 1,
        "iso3": "cod",
        "region_name": "Zone 2",
        "pcodes": [NORDKIVU1, SUDKIVU1],
    },
    {
        "adm_level": 1,
        "iso3": "cod",
        "region_name": "Zone 3",
        "pcodes": [TANGANYIKA1],
    },
]
