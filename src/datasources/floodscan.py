import os
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
import xarray as xr
from tqdm.auto import tqdm

from src.datasources import codab, worldpop
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
    Calculate recent (2025 onwards) flood exposure rasters for a given country.
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
            name_starts_with="raster/cogs/aer_area_300s_",
            container_name="global",
        )
        if x.endswith(".tif")
    ]
    # filter to only 2025 onwards
    recent_fs_raw_files = [
        x for x in existing_fs_raw_files if "300s_2025" in x
    ]
    # check for existing processed exposure rasters
    existing_exposure_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
        f"{iso3}"
    )

    # stack up relevant raw Floodscan rasters
    das = []
    for blob_name in tqdm(recent_fs_raw_files):
        date_in = datetime.strptime(blob_name.split("/")[-1][14:22], "%Y%m%d")
        date_str = date_in.strftime("%Y-%m-%d")
        exposure_blob_name = get_blob_name(
            iso3, "exposure_raster", date=date_str
        )
        if exposure_blob_name in existing_exposure_files and not clobber:
            if verbose:
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


def calculate_recent_flood_exposure_rasterstats(
    iso3: str, clobber: bool = False, verbose: bool = False
):
    """
    Calculate recent (2025 onwards) flood exposure sums for a given country.
    Only calculates for admin level 2.
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
    adm = codab.load_codab_from_blob(iso3, admin_level=2)
    # check for existing exposure rasters
    recent_exposure_rasters = [
        x
        for x in blob.list_container_blobs(
            name_starts_with=f"{blob.PROJECT_PREFIX}/processed/"
            f"flood_exposure/{iso3}/"
        )
        if x.endswith(".tif")
    ]
    # load existing exposure tabular data
    blob_name = get_blob_name(iso3, "exposure_tabular")
    try:
        df_exp_adm_existing = blob.load_parquet_from_blob(blob_name)
    except Exception as e:
        print(e)
        df_exp_adm_existing = pd.DataFrame(columns=["date"])

    existing_dates = df_exp_adm_existing["date"].unique()
    # filter to only unprocessed exposure rasters
    unprocessed_exposure_rasters = [
        x
        for x in recent_exposure_rasters
        if datetime.strptime(x.split("/")[-1][13:23], "%Y-%m-%d")
        not in existing_dates
        or clobber
    ]
    if clobber:
        df_empty = pd.DataFrame(columns=["date"])
        blob_name = get_blob_name(iso3, "exposure_tabular")
        blob.upload_parquet_to_blob(blob_name, df_empty, index=False)

    # break list of exposure rasters into chunks, to avoid memory issues
    # chunk length is arbitrary, but seems to work fine
    chunk_len = 100
    exposure_raster_chunks = [
        unprocessed_exposure_rasters[x : x + chunk_len]
        for x in range(0, len(unprocessed_exposure_rasters), chunk_len)
    ]

    # iterate over chunks
    for exposure_raster_chunk in tqdm(exposure_raster_chunks):
        blob_name = get_blob_name(iso3, "exposure_tabular")
        try:
            df_exp_adm_existing = blob.load_parquet_from_blob(blob_name)
        except Exception as e:
            print(e)
            df_exp_adm_existing = pd.DataFrame(columns=["date"])

        existing_dates = df_exp_adm_existing["date"].unique()
        if verbose:
            print(existing_dates)

        # stack up exposure rasters in chunk
        das = []
        for blob_name in tqdm(exposure_raster_chunk):
            date_in = datetime.strptime(
                blob_name.split("/")[-1][13:23], "%Y-%m-%d"
            )
            if date_in in existing_dates and not clobber:
                if verbose:
                    print(f"already processed for {date_in}, skipping")
                continue
            try:
                da_in = blob.open_blob_cog(blob_name)
                da_in["date"] = date_in
                da_in = da_in.persist()
                das.append(da_in)
            except Exception as e:
                print(e)
                print(f"couldn't open {blob_name}")

        if len(das) == 0:
            print("all complete for chunk")
            continue
        ds_exp_recent = xr.concat(das, dim="date").squeeze(
            dim="band", drop=True
        )
        if verbose:
            print(ds_exp_recent)

        # iterate over admin level 2 regions and calculate exposure sums
        dfs = []
        for pcode, row in tqdm(
            adm.set_index("ADM2_PCODE").iterrows(), total=len(adm)
        ):
            da_clip = ds_exp_recent.rio.clip([row.geometry])
            dff = (
                da_clip.sum(dim=["x", "y"])
                .to_dataframe(name="total_exposed")["total_exposed"]
                .astype(int)
                .reset_index()
            )
            dff["ADM2_PCODE"] = pcode
            dfs.append(dff)

        df_exp_adm_new = pd.concat(dfs, ignore_index=True)
        if verbose:
            print(df_exp_adm_new)

        df_exp_adm_combined = pd.concat(
            [df_exp_adm_existing, df_exp_adm_new], ignore_index=True
        )
        if verbose:
            print(df_exp_adm_combined)

        blob_name = get_blob_name(iso3, "exposure_tabular")
        blob.upload_parquet_to_blob(
            blob_name, df_exp_adm_combined, index=False
        )


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
