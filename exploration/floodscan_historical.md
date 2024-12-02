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

# Floodscan historical

Using local file (quicker than with COGs on blob)

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
from tqdm.auto import tqdm

from src.datasources import worldpop, floodscan, codab
from src.utils import blob
```

```python
# done re-do: TCD, CMR, NER, NGA, BFA, MLI
```

```python
iso3 = "mli"
```

```python
codab.download_codab_to_blob(iso3)
```

```python
adm = codab.load_codab_from_blob(iso3, admin_level=2)
```

```python
adm.plot()
```

```python
worldpop.download_worldpop_to_blob(iso3)
```

```python
pop = worldpop.load_worldpop_from_blob(iso3)
```

```python
pop.plot()
```

```python
fs = floodscan.open_historical_floodscan()
```

```python
fs = fs.rename({"lat": "y", "lon": "x", "time": "date"})
```

```python
fs_filtered = fs.where(fs >= 0.05)
```

```python
exposure = fs_filtered.interp_like(pop, method="nearest") * pop
```

```python
exposure
```

```python
fig, ax = plt.subplots(dpi=200)
adm.boundary.plot(ax=ax, color="k", linewidth=0.2)
exposure.isel(date=-1).plot(ax=ax)
ax.axis("off")
plt.show()
```

```python
verbose = True
clobber = False

existing_exposure_files = blob.list_container_blobs(
    name_starts_with=f"{blob.PROJECT_PREFIX}/processed/flood_exposure/{iso3}/"
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

```python
test = blob.open_blob_cog(blob_name)
```

```python
test.plot()
```

```python
blob_name
```
