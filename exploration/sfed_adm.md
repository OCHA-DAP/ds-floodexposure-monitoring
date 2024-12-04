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

# SFED fraction by admin

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import xarray as xr

from src.datasources import codab, floodscan
from src.utils import blob
```

```python
LOGONECHARI2 = "CM004002"
```

```python
fs = floodscan.open_historical_floodscan()
```

```python
fs
```

## Logone-et-Chari

```python
adm2 = codab.load_codab_from_blob("cmr", admin_level=2)
```

```python
adm2_aoi = adm2[adm2["ADM2_PCODE"] == LOGONECHARI2]
```

```python
fs_clip = fs.rio.clip(adm2_aoi.geometry)
```

```python
fig, ax = plt.subplots()
adm2.boundary.plot(ax=ax)
fs_clip.isel(time=0).plot(ax=ax)
```

```python
fs_clip_mean = fs_clip.mean(dim=["lat", "lon"])
```

```python
df_fs = fs_clip_mean.to_dataframe()
```

```python
df_fs = df_fs["SFED_AREA"].reset_index()
```

```python
df_fs
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/sfed/cmr_logonechari_sfed_historical.parquet"
blob.upload_parquet_to_blob(blob_name, df_fs)
```

```python
blob_name
```

## Makary

```python
MAKARY3 = "CM004002008"
```

```python
adm3 = codab.load_codab_from_blob("cmr", admin_level=3)
```

```python
adm3_aoi = adm3[adm3["ADM3_PCODE"] == MAKARY3]
```

```python
fs_clip3 = fs.rio.clip(adm3_aoi.geometry)
```

```python
fig, ax = plt.subplots()
adm3.boundary.plot(ax=ax)
fs_clip3.isel(time=0).plot(ax=ax)
```

```python
fs_clip_mean3 = fs_clip3.mean(dim=["lat", "lon"])
```

```python
df_fs3 = fs_clip_mean3.to_dataframe()
```

```python
df_fs3 = df_fs3["SFED_AREA"].reset_index()
```

```python
df_fs3
```

```python
df_fs3["SFED_AREA"].hist()
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/sfed/cmr_makary_sfed_historical.parquet"
)
blob.upload_parquet_to_blob(blob_name, df_fs3)
```

## Pixels

```python
fs_clip = fs_clip.persist()
```

```python
fs_clip
```

```python
fig, ax = plt.subplots()
fs_clip.max(dim=["time"]).plot(ax=ax)
adm3.boundary.plot(ax=ax, color="k")
ax.axis("off")
```

```python
max_val = fs_clip.max(dim=["time"]).max().compute()
```

```python
max_val
```

```python
df_pixels = fs_clip.to_dataframe()["SFED_AREA"].reset_index().dropna()
```

```python
df_pixels["grid_id"] = (
    df_pixels["lon"].apply(round, ndigits=3).astype(str)
    + "_"
    + df_pixels["lat"].apply(round, ndigits=3).astype(str)
)
```

```python
df_pixels
```

```python
df_pixels.groupby("grid_id")["SFED_AREA"].mean().reset_index().sort_values(
    "SFED_AREA", ascending=False
).iloc[:20]
```

```python
GRID_ID = "14.208_12.875"
```

```python
df_pixels[df_pixels["grid_id"] == GRID_ID].hist("SFED_AREA", bins=20)
```

```python
df_pixels[df_pixels["grid_id"] == GRID_ID].groupby(df_pixels["time"].dt.year)[
    "SFED_AREA"
].max().reset_index().hist("SFED_AREA")
```

```python
df_pixels
```

```python
blob_name = f"{blob.PROJECT_PREFIX}/processed/sfed/cmr_extremenord_pixels_sfed_historical.parquet"
blob.upload_parquet_to_blob(blob_name, df_pixels)
```

```python
blob_name
```
