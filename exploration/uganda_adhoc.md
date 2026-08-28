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

# Uganda

Grabbing raster stats ad-hoc for some specific dates.

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from datetime import datetime

import pandas as pd
import ocha_stratus as stratus
import xarray as xr

from src.datasources import codab, worldpop, floodscan
from src.constants import *
```

```python
iso3 = "uga"
```

```python
codab.download_codab_to_blob(iso3=iso3)
```

```python
adm0 = codab.load_codab_from_blob(iso3=iso3, admin_level=0)
```

```python
adm0.plot()
```

```python
worldpop.download_worldpop_to_blob(iso3=iso3)
```

```python
da_wp = worldpop.load_worldpop_from_blob(iso3=iso3)
```

```python
da_wp.plot()
```

```python
dates1 = pd.date_range("2022-09-03", "2022-10-21")
dates2 = pd.date_range("2024-04-29", "2024-05-12")
```

```python
dates = dates1.append(dates2)
```

```python
dates
```

```python
FS_BLOB_NAME = (
    "floodscan/daily/v5/processed/aer_area_300s_v{date_str}_v05r01.tif"
)
```

```python
das = []
for date in dates:
    blob_name = FS_BLOB_NAME.format(date_str = date.date())
    da_in = stratus.open_blob_cog(blob_name, stage="prod", container_name="raster")
    da_in = da_in.sel(band=1)
```

```python
blob_names = [FS_BLOB_NAME.format(date_str=date.date()) for date in dates]
```

```python
blob_names
```

```python
floodscan.process_batch_flood_exposure(
    blob_names,
    da_wp,
    iso3=iso3,
    existing_exposure_files=[],
    clobber=False,
    verbose=True,
    read_stage="prod",
    write_stage="dev",
)
```

```python
existing_exposure_rasters = [
    x
    for x in stratus.list_container_blobs(
        name_starts_with=f"{PROJECT_PREFIX}/processed/"
        f"flood_exposure/{iso3}/",
        stage="dev",
    )
    if x.endswith(".tif")
]
```

```python
das = []
for blob_name in existing_exposure_rasters:
    date_in = datetime.strptime(blob_name.split("/")[-1][13:23], "%Y-%m-%d")
    try:
        da_in = stratus.open_blob_cog(blob_name, stage="dev")
        da_in["date"] = date_in
        da_in = da_in.persist()
        das.append(da_in)
    except Exception as e:
        print(e)
        print(f"couldn't open {blob_name}")
```

```python
ds_exp_recent = xr.concat(das, dim="date").squeeze(dim="band", drop=True)
```

```python
da_clip = ds_exp_recent.rio.clip(adm0.geometry)
```

```python
df_exp_raw = (
    da_clip.sum(dim=["x", "y"])
    .to_dataframe(name="total_exposed")["total_exposed"]
    .astype(int)
    .reset_index()
)
```

```python
df_exp_raw
```

```python
roll = 3


def calculate_rolling_group(group):
    group = group.sort_values("date")
    group[f"total_exposed_roll{roll}"] = (
        group["total_exposed"].rolling(roll).mean()
    )
    return group
```

```python
df_exp = (
    df_exp_raw.groupby(df_exp_raw["date"].dt.year)
    .apply(calculate_rolling_group, include_groups=False)
    .reset_index(drop=True)
)
df_exp = df_exp.dropna()
df_exp[f"total_exposed_roll{roll}"] = df_exp[
    f"total_exposed_roll{roll}"
].astype(int)
```

```python
df_exp[df_exp["date"].dt.year == 2024]
```

```python
df_exp.loc[
    df_exp.groupby(df_exp["date"].dt.year)["total_exposed_roll3"].idxmax()
]
```

```python
filename = f"temp/floodscan_exposure_{iso3}_adm0_selectdates.csv"
```

```python
df_exp.to_csv(filename, index=False)
```
