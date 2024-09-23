import os
from datetime import datetime
from pathlib import Path
from typing import Literal

import xarray as xr
from tqdm.auto import tqdm

from src.datasources import worldpop
from src.utils import blob

DATA_DIR = Path(os.getenv("AA_DATA_DIR_NEW", ""))
RAW_FS_HIST_S_PATH = (
    DATA_DIR
    / "private"
    / "raw"
    / "glb"
    / "FloodScan"
    / "SFED"
    / "SFED_historical"
    / "aer_sfed_area_300s_19980112_20231231_v05r01.nc"
)


def calculate_recent_flood_exposure_rasters(
    iso3: str, clobber: bool = False, verbose: bool = False
):
    pop = worldpop.load_worldpop_from_blob(iso3)
    existing_fs_raw_files = [
        x
        for x in blob.list_container_blobs(
            name_starts_with="raster/cogs/aer_area_300s_",
            container_name="global",
        )
        if x.endswith(".tif")
    ]
    recent_fs_raw_files = [
        x for x in existing_fs_raw_files if "300s_2024" in x
    ]
    existing_exposure_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
        f"{iso3}"
    )

    das = []
    for blob_name in tqdm(recent_fs_raw_files):
        date_in = datetime.strptime(blob_name.split("/")[-1][14:22], "%Y%m%d")
        date_str = date_in.strftime("%Y-%m-%d")
        exposure_blob_name = get_blob_name(
            iso3, "exposure_raster", date=date_str
        )
        if exposure_blob_name in existing_exposure_files and not clobber:
            print(f"already processed for {date_str}, skipping")
            continue
        da_in = blob.open_blob_cog(blob_name, container_name="global")
        long_name = da_in.attrs["long_name"]
        if long_name == ("SFED", "MFED"):
            da_in = da_in.isel(band=0)
        elif long_name == ("MFED", "SFED"):
            da_in = da_in.isel(band=1)
        elif long_name == "SFED":
            da_in = da_in.isel(band=0)
        else:
            print(f"unrecognized long_name, skipping {date_in}")
            continue
        da_in = da_in.drop_vars("band")
        da_in["date"] = date_in
        da_in = da_in.persist()
        das.append(da_in)

    if not das:
        if verbose:
            print("no new floodscan data to process")
        return
    ds_recent = xr.concat(das, dim="date")
    ds_recent_filtered = ds_recent.where(ds_recent >= 0.05)
    exposure = ds_recent_filtered.interp_like(pop, method="nearest") * pop
    existing_exposure_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
        f"{iso3}"
    )

    for date in tqdm(exposure.date):
        date_str = str(date.values.astype("datetime64[D]"))
        blob_name = get_blob_name(iso3, "exposure_raster", date=date_str)
        if blob_name in existing_exposure_files and not clobber:
            if verbose:
                print("already processed")
            continue
        print(blob_name)
        blob.upload_cog_to_blob(blob_name, exposure.sel(date=date))


def calculate_recent_flood_exposure_rasterstats(iso3: str):
    pass


def open_historical_floodscan():
    chunks = {"lat": 1080, "lon": 1080, "time": 1}
    ds = xr.open_dataset(RAW_FS_HIST_S_PATH, chunks=chunks)
    da = ds["SFED_AREA"]
    da = da.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    da = da.rio.write_crs(4326)
    return da


def get_blob_name(
    iso3: str,
    data_type: Literal["exposure_raster", "exposure_tabular"],
    date: str = None,
):
    if data_type == "exposure_raster":
        if date is None:
            raise ValueError("date must be provided for exposure data")
        return (
            f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
            f"{iso3}/{iso3}_exposure_{date}.tif"
        )
    elif data_type == "exposure_tabular":
        return (
            f"{blob.PROJECT_PREFIX}/processed/flood_exposure/tabular/"
            f"{iso3}_adm_flood_exposure.parquet"
        )
    elif data_type == "flood_extent":
        return (
            f"{blob.PROJECT_PREFIX}/processed/flood_extent/"
            f"{iso3}_flood_extent.tif"
        )
