from datetime import datetime

import pandas as pd
import xarray as xr
from tqdm.auto import tqdm

from src.constants import ISO3S
from src.datasources import codab
from src.utils import blob, database


def get_existing_stats_dates(iso3: str, engine) -> list:
    query = f"""
    SELECT DISTINCT valid_date
    FROM app.floodscan_exposure
    WHERE iso3 = '{iso3.upper()}'
    ORDER BY valid_date
    """
    df_unique_dates = pd.read_sql(query, con=engine)
    df_unique_dates["valid_date"] = pd.to_datetime(
        df_unique_dates["valid_date"]
    )
    return df_unique_dates["valid_date"].to_list()


if __name__ == "__main__":
    engine = database.get_engine(stage="dev")
    clobber = False
    verbose = False

    for iso3 in ISO3S:
        print(f"Processing {iso3}")
        adm = codab.load_codab_from_blob(iso3, admin_level=2)
        existing_exposure_rasters = [
            x
            for x in blob.list_container_blobs(
                name_starts_with=f"{blob.PROJECT_PREFIX}/processed/"
                f"flood_exposure/{iso3}/"
            )
            if x.endswith(".tif")
        ]
        existing_dates = get_existing_stats_dates(iso3, engine)
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
            for blob_name in tqdm(exposure_raster_chunk):
                date_in = datetime.strptime(
                    blob_name.split("/")[-1][13:23], "%Y-%m-%d"
                )
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
                    df_exp_adm_new.groupby(["date", pcode_col])[
                        "total_exposed"
                    ]
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
                    "floodscan_exposure",
                    schema="app",
                    con=engine,
                    if_exists="append",
                    chunksize=10000,
                    index=False,
                    method=database.postgres_upsert,
                )
