---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.16.1
  kernelspec:
    display_name: ds-floodexposure-monitoring
    language: python
    name: ds-floodexposure-monitoring
---

# Floodscan

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

from tqdm.auto import tqdm

from src.datasources import worldpop, floodscan, codab
from src.utils import blob
```

```python
NDJAMENA2 = "TD1801"
```

```python
# done re-do: TCD, NER, CMR, BFA, NGA, ETH, SOM
```

```python
iso3 = "som"
```

```python
adm = codab.load_codab_from_blob(iso3, admin_level=2)
```

```python
pop = worldpop.load_worldpop_from_blob(iso3)
```

```python
existing_fs_raw_files = [
    x
    for x in blob.list_container_blobs(
        name_starts_with="raster/cogs/aer_area_300s_", container_name="global"
    )
    if x.endswith(".tif")
]
```

```python
recent_fs_raw_files = [x for x in existing_fs_raw_files if "300s_2024" in x]
```

```python
recent_fs_raw_files
```

## Stack up recent FS rasters

```python
# can set to True to test timing
clobber = False

existing_exposure_files = blob.list_container_blobs(
    name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/{iso3}"
)

das = []
for blob_name in tqdm(recent_fs_raw_files):
    date_in = datetime.strptime(blob_name.split("/")[-1][14:22], "%Y%m%d")
    date_str = date_in.strftime("%Y-%m-%d")
    exposure_blob_name = floodscan.get_blob_name(
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

ds_recent = xr.concat(das, dim="date")
```

```python
ds_recent
```

```python
ds_recent_filtered = ds_recent.where(ds_recent >= 0.05)
```

```python
exposure = ds_recent_filtered.interp_like(pop, method="nearest") * pop
```

```python
fig, ax = plt.subplots(dpi=200)
adm.boundary.plot(ax=ax, color="k", linewidth=0.2)
ds_recent_filtered.rio.reproject_match(pop).rio.clip(adm.geometry).isel(
    date=-1
).plot(ax=ax)
ax.axis("off")
plt.show()
```

```python
fig, ax = plt.subplots(dpi=200)
adm.boundary.plot(ax=ax, color="k", linewidth=0.2)
ds_recent_filtered.interp_like(pop, method="nearest").rio.clip(
    adm.geometry
).isel(date=-1).plot(ax=ax)
ax.axis("off")
plt.show()
```

```python
fig, ax = plt.subplots(dpi=200)
adm.boundary.plot(ax=ax, color="k", linewidth=0.2)
exposure.rio.clip(adm.geometry).isel(date=-1).plot(ax=ax)
ax.axis("off")
plt.show()
```

```python
existing_exposure_files = blob.list_container_blobs(
    name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/{iso3}"
)
```

```python
existing_exposure_files[-1]
```

```python
verbose = False
clobber = False

existing_exposure_files = blob.list_container_blobs(
    name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/{iso3}"
)

for date in tqdm(exposure.date):
    date_str = str(date.values.astype("datetime64[D]"))
    blob_name = floodscan.get_blob_name(iso3, "exposure_raster", date=date_str)
    if blob_name in existing_exposure_files and not clobber:
        if verbose:
            print("already processed")
        continue
    print(blob_name)
    blob.upload_cog_to_blob(blob_name, exposure.sel(date=date))
```

## Calculating raster stats

### Building up exposure `ds`

```python
# done re-do: TCD, NER, CMR, SOM
```

```python
recent_exposure_rasters = [
    x
    for x in blob.list_container_blobs(
        name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/{iso3}/"
    )
    if x.endswith(".tif")
]
```

```python
blob_name = floodscan.get_blob_name(iso3, "exposure_tabular")
try:
    df_exp_adm_existing = blob.load_parquet_from_blob(blob_name)
except Exception as e:
    print(e)
    df_exp_adm_existing = pd.DataFrame(columns=["date"])

existing_dates = df_exp_adm_existing["date"].unique()
```

```python
df_exp_adm_existing
```

```python
df_exp_adm_existing.groupby(["date", "ADM2_PCODE"])["total_exposed"].size()
```

```python
existing_dates
```

```python
clobber = False
unprocessed_exposure_rasters = [
    x
    for x in recent_exposure_rasters
    if datetime.strptime(x.split("/")[-1][13:23], "%Y-%m-%d")
    not in existing_dates
    or clobber
]
```

```python
df_exp_adm_existing.groupby(df_exp_adm_existing["date"].dt.year)[
    "total_exposed"
].sum().plot()
```

```python
unprocessed_exposure_rasters
```

```python
clobber = False

if clobber:
    df_empty = pd.DataFrame(columns=["date"])
    blob_name = floodscan.get_blob_name(iso3, "exposure_tabular")
    blob.upload_parquet_to_blob(blob_name, df_empty, index=False)

chunk_len = 100
exposure_raster_chunks = [
    unprocessed_exposure_rasters[x : x + chunk_len]
    for x in range(0, len(unprocessed_exposure_rasters), chunk_len)
]

verbose = False
for exposure_raster_chunk in tqdm(exposure_raster_chunks):
    blob_name = floodscan.get_blob_name(iso3, "exposure_tabular")
    try:
        df_exp_adm_existing = blob.load_parquet_from_blob(blob_name)
    except Exception as e:
        print(e)
        df_exp_adm_existing = pd.DataFrame(columns=["date"])

    existing_dates = df_exp_adm_existing["date"].unique()
    if verbose:
        print(existing_dates)

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
    ds_exp_recent = xr.concat(das, dim="date").squeeze(dim="band", drop=True)
    if verbose:
        display(ds_exp_recent)

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
        display(df_exp_adm_new)

    df_exp_adm_combined = pd.concat(
        [df_exp_adm_existing, df_exp_adm_new], ignore_index=True
    )
    if verbose:
        display(df_exp_adm_combined)

    blob_name = floodscan.get_blob_name(iso3, "exposure_tabular")
    blob.upload_parquet_to_blob(blob_name, df_exp_adm_combined, index=False)
```

```python

```
