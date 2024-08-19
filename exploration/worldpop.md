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

# WorldPop

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import worldpop

from src.constants import *
```

```python
for iso3 in ISO3S:
    worldpop.download_worldpop_to_blob(iso3)
```

```python
test = worldpop.load_worldpop_from_blob("tcd")
```

```python
test.plot(vmax=200)
```

```python

```
