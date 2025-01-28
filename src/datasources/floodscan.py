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
    """
    Calculate recent flood exposure rasters for a given country.
    Only looks to update data from the current year onwards.

    Parameters
    ----------
    iso3: str
        ISO3 code of the country
    clobber: bool
        Whether to overwrite existing data
    verbose: bool
        Whether to print progress of specific dates

    Returns
    -------
    """
    pop = worldpop.load_worldpop_from_blob(iso3)
    # check for existing raw Floodscan rasters
    existing_fs_raw_files = [
        x
        for x in blob.list_container_blobs(
            name_starts_with=f"{blob.FLOODSCAN_COG_FILEPATH}/aer_area_300s",
            container_name="raster",
        )
        if x.endswith(".tif")
    ]
    # filter to only this year onwards
    this_year = datetime.today().year
    recent_fs_raw_files = [
        x for x in existing_fs_raw_files if f"300s_v{this_year}" in x
    ]
    # check for existing processed exposure rasters
    existing_exposure_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
        f"{iso3}"
    )

    # stack up relevant raw Floodscan rasters
    das = []
    for blob_name in tqdm(recent_fs_raw_files):
        date_in = datetime.strptime(
            blob_name.split("/")[-1][15:25], "%Y-%m-%d"
        )
        date_str = date_in.strftime("%Y-%m-%d")
        exposure_blob_name = get_blob_name(
            iso3, "exposure_raster", date=date_str
        )
        if exposure_blob_name in existing_exposure_files and not clobber:
            if verbose:
                print(f"already processed for {date_str}, skipping")
            continue
        da_in = blob.open_blob_cog(blob_name, container_name="raster")
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
    # filter to only pixels with flood extent > 5% to reduce noise
    ds_recent_filtered = ds_recent.where(ds_recent >= 0.05)
    # interpolate to Worldpop grid and
    # multiply by population to get exposure
    exposure = ds_recent_filtered.interp_like(pop, method="nearest") * pop
    existing_exposure_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
        f"{iso3}"
    )

    # iterate over dates and upload COGs to blob storage
    for date in tqdm(exposure.date):
        date_str = str(date.values.astype("datetime64[D]"))
        blob_name = get_blob_name(iso3, "exposure_raster", date=date_str)
        if blob_name in existing_exposure_files and not clobber:
            if verbose:
                print("already processed")
            continue
        if verbose:
            print(f"uploading {blob_name}")
        blob.upload_cog_to_blob(blob_name, exposure.sel(date=date))


def open_historical_floodscan():
    """
    Open the historical Floodscan dataset, as a netCDF on the
    Google Drive.
    Returns
    -------
    xr.DataArray
        Floodscan dataset
    """
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
    """
    Get the blob name for a given data type and date.
    Parameters
    ----------
    iso3: str
        ISO3 code of the country
    data_type: Literal["exposure_raster", "exposure_tabular"]
        Type of data (exposure_raster is daily raster of the country,
        exposure_tabular is a table of daily exposure sums by admin2)
    date: str
        Date of the exposure raster, in "YYYY-MM-DD" format
        Not relevant for exposure_tabular

    Returns
    -------
    str
        Blob name
    """
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
