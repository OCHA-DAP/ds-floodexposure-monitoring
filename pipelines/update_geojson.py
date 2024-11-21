import geopandas as gpd
import pandas as pd

from src.constants import ISO3S
from src.datasources import codab
from src.utils import blob

ADMS = [0, 1, 2]


def clean_gdf(gdf):
    gdf["name"] = gdf.apply(
        lambda row: (
            row[f"ADM{adm}_EN"]
            if pd.notna(row.get(f"ADM{adm}_EN"))
            and row.get(f"ADM{adm}_EN") != ""
            else row.get(f"ADM{adm}_FR", "")
        ),
        axis=1,
    )
    gdf.rename(columns={f"ADM{adm}_PCODE": "pcode"}, inplace=True)
    gdf = gdf[["pcode", "name", "geometry"]]
    return gdf


if __name__ == "__main__":
    for adm in ADMS:
        print(f"Processing geo data for admin {adm}...")
        gdfs = []
        out_file = f"pipelines/adm{adm}.json"
        for iso3 in ISO3S:
            gdf = codab.load_codab_from_blob(iso3, admin_level=adm)
            gdfs.append(gdf)
        gdf_all = pd.concat(gdfs)
        if adm == 0:
            gdf_all_outline = gdf_all.copy()
            gdf_all_outline.geometry = gdf_all_outline.geometry.boundary
            gdf_all_outline = gpd.GeoDataFrame(
                gdf_all_outline, geometry="geometry"
            )
            gdf_all_outline = clean_gdf(gdf_all_outline)
            blob.upload_gdf_as_geojson(
                f"ds-floodexposure-monitoring/processed/geojson/adm{adm}_outline.json",  # noqa
                gdf_all_outline,
                "dev",
                "projects",
            )

        gdf_all.geometry = gdf_all.geometry
        gdf_all = gpd.GeoDataFrame(gdf_all, geometry="geometry")
        gdf_all = clean_gdf(gdf_all)

        blob.upload_gdf_as_geojson(
            f"ds-floodexposure-monitoring/processed/geojson/adm{adm}.json",
            gdf_all,
            "dev",
            "projects",
        )
    print("All data processed and uploaded to blob.")
