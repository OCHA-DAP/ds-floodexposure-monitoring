import ocha_stratus as stratus

from src.constants import ISO3S, REGIONS, STAGE
from src.datasources import floodscan
from src.utils import database

if __name__ == "__main__":

    clobber = False
    verbose = False
    engine = stratus.get_engine(stage=STAGE)
    table_name = "floodscan_exposure"
    table_name_regions = "floodscan_exposure_regions"

    # updates per iso3
    database.create_flood_exposure_table(table_name, engine)
    for iso3 in ISO3S:
        print(f"Processing {iso3}")
        floodscan.calculate_flood_exposure_rasterstats(
            iso3=iso3,
            engine=engine,
            clobber=clobber,
            verbose=verbose,
            output_table=table_name,
        )

    # updates per region
    database.create_flood_exposure_table(table_name_regions, engine)
    for region in REGIONS:
        print(f"Processing {region['iso3']} region {region['region_number']}")
        floodscan.calculate_flood_exposure_rasterstats_regions(
            region=region, engine=engine, output_table=table_name_regions
        )
