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

# Comparison with absolute SFED

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt

from src.datasources import floodscan, codab
from src.utils import blob
```

```python
iso3 = "nga"
```

```python
adm = codab.load_codab_from_blob(iso3=iso3)
adm = adm[[True, False]]
```

```python
adm.plot()
```

```python
df_exp = blob.load_parquet_from_blob(
    floodscan.get_blob_name(iso3=iso3, data_type="exposure_tabular")
)
```

```python
df_exp_adm0 = df_exp.groupby("date")["total_exposed"].sum().reset_index()
```

```python
df_exp_adm0
```

```python
fs = floodscan.open_historical_floodscan()
```

```python
fs_clip = fs.rio.clip(adm.geometry)
```

```python
fs_clip
```

```python
fs_clip = fs_clip.persist()
```

```python
fig, ax = plt.subplots()
adm.boundary.plot(ax=ax)
fs_clip.isel(time=0).plot(ax=ax)
```

```python
fs_clip
```

```python
10 * 9485 / 100 / 60
```

```python
%time fs_clip.isel(time=slice(0, 20)).mean(dim=["lat", "lon"]).to_dataframe()["SFED_AREA"].reset_index()
```

```python
df_sfed = (
    fs_clip.mean(dim=["lat", "lon"]).to_dataframe()["SFED_AREA"].reset_index()
)
```

```python
df_sfed
```

```python
blob_name = (
    f"{blob.PROJECT_PREFIX}/processed/sfed/{iso3}_adm0_sfed_historical.parquet"
)
blob.upload_parquet_to_blob(blob_name, df_sfed)
```

```python
df_sfed
```

```python
df_exp_adm0
```

```python
df_compare = df_sfed.rename(columns={"time": "date"}).merge(df_exp_adm0)
```

```python
fig, ax = plt.subplots(dpi=300)
df_compare.plot(
    x="SFED_AREA",
    y="total_exposed",
    marker=".",
    linestyle="",
    markersize=1,
    ax=ax,
)
```

```python
fig, ax = plt.subplots(dpi=300, figsize=(8, 6))

# Plotting with transparency to avoid overplotting
df_compare.plot(
    x="SFED_AREA",
    y="total_exposed",
    marker=".",
    linestyle="",
    markersize=2,
    alpha=0.1,  # Set transparency
    ax=ax,
)
```

```python
df_compare.groupby(df_compare["date"].dt.year).max().drop(
    columns="date"
).reset_index()
```

```python
fig, ax = plt.subplots(dpi=300)
df_compare.groupby(df_compare["date"].dt.year).max().drop(
    columns="date"
).reset_index().plot(
    x="SFED_AREA",
    y="total_exposed",
    marker=".",
    linestyle="",
    markersize=2,
    ax=ax,
)
```
