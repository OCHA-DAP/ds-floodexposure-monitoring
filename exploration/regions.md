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

# Regions

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.utils import database
from src.constants import *
```

```python
engine = database.get_engine()
```

```python
database.create_flood_exposure_region_table(
    "floodscan_exposure_regions", database.get_engine()
)
```

```python
for region in REGIONS:
    print(region)
```

```python
query = """
SELECT *
FROM app.floodscan_exposure
WHERE iso=:pcode AND adm_level=:adm_level
"""
```

```python
query
```
