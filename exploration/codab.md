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

# CODAB

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
from src.datasources import codab
from src.constants import *
```

```python
codab.download_codab_to_blob("nga")
```

```python
for iso3 in ISO3S:
    print(iso3)
    codab.download_codab_to_blob(iso3)
```

```python
for iso3 in ISO3S:
    adm = codab.load_codab_from_blob(iso3)
    adm.plot()
```

```python
tcd_test = codab.load_codab_from_blob("tcd")
```

```python
tcd_test.dissolve().plot()
```

```python
tcd_test[[True, False]].plot()
```

```python
tcd_test[[False, True]].plot()
```
