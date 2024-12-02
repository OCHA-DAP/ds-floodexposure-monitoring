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

# Removing parquet

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import os

import pandas as pd
from sqlalchemy import create_engine

from src.utils import database, blob
from src.datasources import floodscan, codab
from src.constants import *
```

```python
iso3 = "cmr"
```

```python
adm2 = codab.load_codab_from_blob(iso3, admin_level=2)
```

```python
adm2.plot()
```

```python
AZURE_DB_PW_DEV = os.getenv("AZURE_DB_PW_DEV")
AZURE_DB_UID = os.getenv("AZURE_DB_UID")
```

```python
engine = database.get_engine()
```

```python
database.create_flood_exposure_table("floodscan_exposure", engine)
```

```python
query = f"""
SELECT DISTINCT date
FROM app.flood_exposure
WHERE iso3 = '{iso3}'
ORDER BY date
"""
df_unique_dates = pd.read_sql(query, con=engine)
```

```python
df_unique_dates["date"] = pd.to_datetime(df_unique_dates["date"])
```

```python
df_unique_dates
```

## Populate historical

```python
blob_name = floodscan.get_blob_name(iso3, "exposure_tabular")
```

```python
df_historical = blob.load_parquet_from_blob(blob_name)
```

```python
df_historical = df_historical.merge(
    adm2[[x for x in adm2.columns if "PCODE" in x]]
)
```

```python
for adm_level in [0, 1, 2]:
    print(adm_level)
    pcode_col = f"ADM{adm_level}_PCODE"
    df_agg = (
        df_historical.groupby(["date", pcode_col])["total_exposed"]
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
    display(df_agg)
    df_agg.to_sql(
        "floodscan_exposure",
        schema="app",
        con=engine,
        if_exists="append",
        chunksize=10000,
        index=False,
        method=database.postgres_upsert,
    )
```

### Check timing

```python
%%time
query = f"""
SELECT *
FROM app.floodscan_exposure
WHERE pcode = '{LOGONEETCHARI2}'
ORDER BY valid_date
"""
df_test = pd.read_sql(query, con=engine)
df_test
```

```python
%%time
query = f"""
SELECT *
FROM app.floodscan_exposure
WHERE pcode = '{EXTREMENORD1}'
ORDER BY valid_date
"""
df_test = pd.read_sql(query, con=engine)
df_test
```

```python
%%time
query = f"""
SELECT *
FROM app.floodscan_exposure
WHERE pcode = 'CM'
ORDER BY valid_date
"""
df_test = pd.read_sql(query, con=engine)
df_test
```

### Check against existing app

```python
df_test["valid_date"] = pd.to_datetime(df_test["valid_date"])
df_plot = df_test[df_test["valid_date"].dt.year == 2024]
```

```python
df_plot.plot(x="valid_date", y="sum")
```

```python
df_plot["roll7"] = df_plot["sum"].rolling(window=7).mean()
```

```python
df_plot.max()
```

```python

```
