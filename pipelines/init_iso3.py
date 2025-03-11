import argparse

from src.constants import ISO3S, REGIONS
from src.datasources import codab, floodscan, worldpop

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--iso3", type=str)
    args = parser.parse_args()
    input_iso3 = args.iso3

    # Confirm that the input ISO3 code is in the ISO3s list
    if input_iso3 not in ISO3S:
        raise ValueError(
            f"{input_iso3} not in ISO3S list. Please make sure to add."
        )

    # Update the `admin_lookup` table for all ISO3s
    codab.load_geo_data(ISO3S, REGIONS, save_to_database=True)

    # Now initialize the data for the input ISO3
    if input_iso3 == "all":
        print("Initializing for all available ISO3s")
        iso3s = ISO3S
    else:
        iso3s = [input_iso3]

    for iso3 in iso3s:
        print(f"Initializing data for {iso3}...")
        codab.download_codab_to_blob(iso3)
        worldpop.download_worldpop_to_blob(iso3)
        floodscan.calculate_flood_exposure_rasters(iso3=iso3, recent=False)

    print("Done!")
