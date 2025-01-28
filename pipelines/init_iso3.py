import argparse

from src.constants import ISO3S
from src.datasources import codab, floodscan, worldpop

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--iso3", type=str)
    args = parser.parse_args()
    input_iso3 = args.iso3

    if not input_iso3:
        print("Initializing for all available ISO3s")
        iso3s = ISO3S
    else:
        iso3s = [input_iso3]

    for iso3 in iso3s:
        print(f"Initializing data for {iso3}...")
        codab.download_codab_to_blob(iso3)
        worldpop.download_worldpop_to_blob(iso3)
        floodscan.calculate_flood_exposure_rasters(iso3=iso3, recent=True)

    print("Done!")
