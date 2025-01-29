from src.constants import ISO3S
from src.datasources import floodscan

if __name__ == "__main__":

    recent = False
    clobber = False
    verbose = False

    for iso3 in ISO3S:
        print(f"Processing {iso3}")
        floodscan.calculate_flood_exposure_rasters(
            iso3=iso3, clobber=clobber, recent=recent, verbose=verbose
        )
