from src.constants import ISO3S
from src.datasources.floodscan import (
    calculate_recent_flood_exposure_rasters,
    calculate_recent_flood_exposure_rasterstats,
)

if __name__ == "__main__":
    for iso3 in ISO3S:
        print(f"Processing {iso3}")
        calculate_recent_flood_exposure_rasters(iso3)
        calculate_recent_flood_exposure_rasterstats(iso3)
