from src.datasources.floodscan import (
    calculate_recent_flood_exposure_rasters,
    calculate_recent_flood_exposure_rasterstats,
)

if __name__ == "__main__":
    calculate_recent_flood_exposure_rasters()
    calculate_recent_flood_exposure_rasterstats()
