from datetime import datetime
from typing import Literal

import pandas as pd
import xarray as xr
from sqlalchemy.engine import Engine
from tqdm.auto import tqdm

from src.constants import STAGE
from src.datasources import codab, worldpop
from src.utils import blob, database


def calculate_flood_exposure_rasters(
    iso3: str,
    clobber: bool = False,
    recent: bool = True,
    verbose: bool = False,
    batch_size: int = 100,
):
    """
    Calculate flood exposure rasters for a given country.

    Parameters
    ----------
    iso3: str
        ISO3 code of the country
    clobber: bool
        Whether to overwrite existing data
    recent: bool
        Whether to look only for data from the current year
    verbose: bool
        Whether to print progress of specific dates
    batch_size: int
        Maximum number of files to process in a single batch (default: 100)

    Returns
    -------
    """
    pop = worldpop.load_worldpop_from_blob(iso3)
    # check for existing raw Floodscan rasters
    existing_fs_raw_files = [
        x
        for x in blob.list_container_blobs(
            name_starts_with=blob.FLOODSCAN_COG_FILEPATH,
            container_name="raster",
            stage=STAGE,
        )
        if x.endswith(".tif")
    ]

    # filter to only this year onwards
    if recent:
        this_year = datetime.today().year
        fs_raw_files = [
            x for x in existing_fs_raw_files if f"300s_v{this_year}" in x
        ]
    # or check all files
    else:
        fs_raw_files = existing_fs_raw_files

    # check for existing processed exposure rasters
    existing_exposure_files = blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/"
        f"{iso3}",
        stage=STAGE,
    )

    # Split files into batches of size batch_size
    total_files = len(fs_raw_files)
    print(f"Total files to process: {total_files}")

    for batch_start in tqdm(range(0, total_files, batch_size)):
        batch_end = min(batch_start + batch_size, total_files)
        current_batch = fs_raw_files[batch_start:batch_end]

        if verbose:
            print(
                f"""Processing batch {batch_start//batch_size + 1},
                files {batch_start+1}-{batch_end} of {total_files}"""
            )

        # Process current batch
        process_batch_flood_exposure(
            current_batch, pop, iso3, existing_exposure_files, clobber, verbose
        )


def process_batch_flood_exposure(
    file_batch, pop, iso3, existing_exposure_files, clobber, verbose
):
    """Process a batch of files"""
    # stack up relevant raw Floodscan rasters for this batch
    das = []
    for blob_name in file_batch:
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
        da_in = blob.open_blob_cog(
            blob_name, container_name="raster", stage=STAGE
        )
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
            print("no new floodscan data to process in this batch")
        return
    ds_recent = xr.concat(das, dim="date")
    # filter to only pixels with flood extent > 5% to reduce noise
    ds_recent_filtered = ds_recent.where(ds_recent >= 0.05)
    # interpolate to Worldpop grid and
    # multiply by population to get exposure
    exposure = ds_recent_filtered.interp_like(pop, method="nearest") * pop

    # iterate over dates and upload COGs to blob storage
    for date in exposure.date:
        date_str = str(date.values.astype("datetime64[D]"))
        blob_name = get_blob_name(iso3, "exposure_raster", date=date_str)
        if blob_name in existing_exposure_files and not clobber:
            if verbose:
                print("already processed")
            continue
        if verbose:
            print(f"uploading {blob_name}")
        blob.upload_cog_to_blob(
            blob_name, exposure.sel(date=date), stage=STAGE
        )


def calculate_flood_exposure_rasterstats(
    iso3: str,
    engine: Engine,
    clobber: bool = False,
    verbose: bool = False,
    output_table: str = "floodscan_exposure",
):
    """
    Calculate flood exposure statistics from raster data for a given country.

    This function processes flood exposure raster data for a specified country,
    calculates exposure statistics at different administrative levels,
    and stores the results in a PostgreSQL database.

    Parameters
    ----------
    iso3 : str
        Three-letter ISO country code
    engine : sqlalchemy.engine.Engine
        SQLAlchemy database engine for PostgreSQL connection
    clobber : bool, optional
        If True, reprocess existing dates. Default is False
    verbose : bool, optional
        If True, print additional processing information. Default is False
    output_table : str, optional
        Name of the output database table. Default is "floodscan_exposure"

    Returns
    -------
    None
        Results are written directly to the database

    """
    adm = codab.load_codab_from_blob(iso3, admin_level=2)
    existing_exposure_rasters = [
        x
        for x in blob.list_container_blobs(
            name_starts_with=f"{blob.PROJECT_PREFIX}/processed/"
            f"flood_exposure/{iso3}/",
            stage=STAGE,
        )
        if x.endswith(".tif")
    ]
    existing_dates = database.get_existing_stats_dates(iso3, engine)
    unprocessed_exposure_rasters = [
        x
        for x in existing_exposure_rasters
        if datetime.strptime(x.split("/")[-1][13:23], "%Y-%m-%d")
        not in existing_dates
        or clobber
    ]

    # break list of exposure rasters into chunks, to avoid memory issues
    # chunk length is arbitrary, but seems to work fine
    chunk_len = 100
    exposure_raster_chunks = [
        unprocessed_exposure_rasters[x : x + chunk_len]
        for x in range(0, len(unprocessed_exposure_rasters), chunk_len)
    ]

    # iterate over chunks
    for exposure_raster_chunk in tqdm(exposure_raster_chunks):
        # stack up exposure rasters in chunk
        das = []
        for blob_name in exposure_raster_chunk:
            date_in = datetime.strptime(
                blob_name.split("/")[-1][13:23], "%Y-%m-%d"
            )
            try:
                da_in = blob.open_blob_cog(blob_name, stage=STAGE)
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
        for pcode, row in adm.set_index("ADM2_PCODE").iterrows():
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

        # aggregate to admin levels and upload
        df_exp_adm_new = df_exp_adm_new.merge(
            adm[[x for x in adm.columns if "PCODE" in x]]
        )
        if verbose:
            print("new raster stats calculated:")
            print(df_exp_adm_new)

        for adm_level in [0, 1, 2]:
            if verbose:
                print("aggregating to adm level:")
                print(adm_level)
            pcode_col = f"ADM{adm_level}_PCODE"
            df_agg = (
                df_exp_adm_new.groupby(["date", pcode_col])["total_exposed"]
                .sum()
                .reset_index()
            )
            df_agg["adm_level"] = adm_level
            df_agg["iso3"] = iso3.upper()
            df_agg = df_agg.rename(
                columns={
                    "total_exposed": "sum",
                    pcode_col: "pcode",
                    "date": "valid_date",
                }
            )
            if verbose:
                print("uploading to DB:")
                print(df_agg)
            df_agg.to_sql(
                output_table,
                schema="app",
                con=engine,
                if_exists="append",
                chunksize=10000,
                index=False,
                method=database.postgres_upsert,
            )


def calculate_flood_exposure_rasterstats_regions(
    region: dict,
    engine: Engine,
    output_table: str = "floodscan_exposure_regions",
):
    print(f"Processing {region['iso3']} region {region['region_number']}")
    adm_stats_df = database.get_existing_adm_stats(region["pcodes"], engine)
    region_stats_df = (
        adm_stats_df.groupby("valid_date")["sum"].sum().reset_index()
    )
    region_stats_df["iso3"] = region["iso3"].upper()
    region_stats_df["pcode"] = (
        f'{region["iso3"]}_region_{region["region_number"]}'
    )
    region_stats_df["adm_level"] = "region"

    region_stats_df.to_sql(
        output_table,
        schema="app",
        con=engine,
        if_exists="append",
        chunksize=10000,
        index=False,
        method=database.postgres_upsert,
    )


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
