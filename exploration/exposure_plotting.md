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

# Exposure plotting

```python
%load_ext jupyter_black
%load_ext autoreload
%autoreload 2
```

```python
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker

from tqdm.auto import tqdm

from src.datasources import worldpop, floodscan, codab
from src.utils import blob
from src.constants import *
```

```python
NDJAMENA2 = "TD1801"
NDJAMENA1 = "TD18"
SILA1 = "TD21"
LAC1 = "TD07"
GAYA2 = "NE003006"
KOLLO2 = "NE006008"
NGUIGMI2 = "NE002006"
DIFFA1 = "NE002"
TAHOUA1 = "NE005"
MARADI1 = "NE004"
TIBESTI1 = "TD22"
NIAMEY1 = "NE008"
LOGONECHARI2 = "CM004002"
MAYODANAY2 = "CM004003"
EXTREMENORD1 = "CM004"
MAYOTSANAGA2 = "CM004006"
MAYOKANI2 = "CM004004"
DIAMARE2 = "CM004001"
```

```python
iso3 = "eth"
```

```python
adm = codab.load_codab_from_blob(iso3, admin_level=2)
```

```python
adm1 = codab.load_codab_from_blob(iso3, admin_level=1)
```

```python
adm3 = codab.load_codab_from_blob(iso3, admin_level=3)
```

```python
pop = worldpop.load_worldpop_from_blob(iso3)
```

```python
blob_name = floodscan.get_blob_name(iso3, "exposure_tabular")
df = blob.load_parquet_from_blob(blob_name)
df = df.merge(adm[["ADM1_PCODE", "ADM2_PCODE"]])
df = df.sort_values("date")
```

```python
df
```

```python
def calculate_rolling(group, window=7):
    group[f"roll{window}"] = (
        group["total_exposed"].rolling(window=window).mean()
    )
    return group
```

```python
window = 7
df = (
    df.groupby("ADM2_PCODE")
    .apply(calculate_rolling, window=window, include_groups=False)
    .reset_index(level=0)
)
```

```python
df["dayofyear"] = df["date"].dt.dayofyear
```

```python
df["eff_date"] = pd.to_datetime(df["dayofyear"], format="%j")
```

```python
most_recent_date_str = f"{df['date'].max():%Y-%m-%d}"
```

```python
most_recent_date_str
```

```python
val_col = f"roll{window}"

seasonal = (
    df[df["date"].dt.year < 2024]
    .groupby(["ADM1_PCODE", "ADM2_PCODE", "dayofyear"])[val_col]
    .mean()
    .reset_index()
)
seasonal["eff_date"] = pd.to_datetime(seasonal["dayofyear"], format="%j")
seasonal
```

```python
today_dayofyear = df.iloc[-1]["dayofyear"]
df_to_today = df[df["dayofyear"] <= today_dayofyear]
```

```python
today_dayofyear
```

```python
df_past_month = df_to_today[df_to_today["dayofyear"] >= today_dayofyear - 30]
```

```python
up_to_today = True
past_month_only = False

df_for_peaks = df_to_today if up_to_today else df
df_for_peaks = df_past_month if past_month_only else df_for_peaks

peak_anytime = (
    df_for_peaks.groupby(
        [df_for_peaks["date"].dt.year, "ADM1_PCODE", "ADM2_PCODE"]
    )[val_col]
    .max()
    .reset_index()
)
```

```python
# ADM2

adm2_pcode = MAYODANAY2

adm_name = adm[adm["ADM2_PCODE"] == adm2_pcode].iloc[0]["ADM2_FR"]

dff = df[df["ADM2_PCODE"] == adm2_pcode].sort_values("date", ascending=False)
seasonal_f = seasonal[seasonal["ADM2_PCODE"] == adm2_pcode]

peak_anytime_f = peak_anytime[peak_anytime["ADM2_PCODE"] == adm2_pcode].copy()
```

```python
# ADM1

adm1_pcode = NDJAMENA1

adm_name = adm[adm["ADM1_PCODE"] == adm1_pcode].iloc[0]["ADM1_FR"]

dff = (
    df[df["ADM1_PCODE"] == adm1_pcode]
    .sort_values("date", ascending=False)
    .groupby(["dayofyear", "date"])[val_col]
    .sum()
    .reset_index()
    .sort_values("date", ascending=False)
)
dff["eff_date"] = pd.to_datetime(dff["dayofyear"], format="%j")

seasonal_f = (
    seasonal[seasonal["ADM1_PCODE"] == adm1_pcode]
    .groupby("eff_date")[val_col]
    .sum()
    .reset_index()
)

peak_anytime_f = (
    peak_anytime[peak_anytime["ADM1_PCODE"] == adm1_pcode]
    .groupby("date")[val_col]
    .sum()
    .reset_index()
)
```

```python
# ADM0

adm_name = adm.iloc[0]["ADM0_FR"]

dff = (
    df.groupby(["dayofyear", "date"])[val_col]
    .sum()
    .reset_index()
    .sort_values("date", ascending=False)
)
dff["eff_date"] = pd.to_datetime(dff["dayofyear"], format="%j")

seasonal_f = seasonal.groupby("eff_date")[val_col].sum().reset_index()

peak_anytime_f = peak_anytime.groupby("date")[val_col].sum().reset_index()
```

```python
# process for plotting
rp = 3
# peaks = dff.groupby(dff["date"].dt.year)[val_col].max().reset_index()
# peaks[f"{rp}yr_rp"] = peaks[val_col] > peaks[val_col].quantile(1 - 1 / rp)
# peak_years = peaks[peaks[f"{rp}yr_rp"]]["date"].to_list()
# peaks["rank"] = peaks[val_col].rank(ascending=False)
# peaks["rp"] = len(peaks) / peaks["rank"]
peak_anytime_f["rank"] = peak_anytime_f[val_col].rank(ascending=False)
peak_anytime_f["rp"] = len(peak_anytime_f) / peak_anytime_f["rank"]
peak_anytime_f[f"{rp}yr_rp"] = peak_anytime_f["rp"] >= rp
peak_years = peak_anytime_f[peak_anytime_f[f"{rp}yr_rp"]]["date"].to_list()
```

```python
# time series
fig = go.Figure()

# seasonal
fig.add_trace(
    go.Scatter(
        x=seasonal_f["eff_date"],
        y=seasonal_f[val_col],
        name="Average",
        line_color="black",
        line_width=2,
    )
)

# past years
for year in dff["date"].dt.year.unique():
    if year == 2024:
        color = CHD_GREEN
        linewidth = 3
    elif year in peak_years:
        color = "red"
        linewidth = 0.2
    else:
        color = "grey"
        linewidth = 0.2
    dff_year = dff[dff["date"].dt.year == year]
    fig.add_trace(
        go.Scatter(
            x=dff_year["eff_date"],
            y=dff_year[val_col],
            name=str(year),
            line_color=color,
            line_width=linewidth,
        )
    )


fig.update_layout(
    template="simple_white",
    xaxis=dict(tickformat="%b %d", dtick="M1"),
    width=800,
    height=600,
    title=f"{adm_name} - time series",
    legend_title="Year",
    margin={"t": 50, "l": 0, "r": 0, "b": 0},
)
fig.update_yaxes(rangemode="tozero", title="Population exposed to flooding")
fig.update_xaxes(title="Date")
fig.show()
```

```python
# return period
peak_anytime_f = peak_anytime_f.sort_values("rp")

fig = go.Figure()
# all years
fig.add_trace(
    go.Scatter(
        x=peak_anytime_f["rp"],
        y=peak_anytime_f[val_col],
        mode="lines",
        line_color="black",
    )
)
# 2024
peak_2024 = peak_anytime_f.set_index("date").loc[2024]
if peak_2024["rank"] == 1:
    position = "bottom left"
elif peak_2024["rank"] == len(peak_anytime_f):
    position = "top right"
else:
    position = "bottom right"

fig.add_trace(
    go.Scatter(
        x=[peak_2024["rp"]],
        y=[peak_2024[val_col]],
        text=f"2024:<br>Exposure = {int(peak_2024[val_col]):,} people<br>"
        f"Return period = {peak_2024['rp']:.1f} years",
        textposition=position,
        mode="markers+text",
        marker_color=CHD_GREEN,
        textfont=dict(size=15, color=CHD_GREEN),
        marker_size=10,
    )
)

# other bad years
rp_peaks = peak_anytime_f[
    (peak_anytime_f[f"{rp}yr_rp"]) & (peak_anytime_f["date"] != 2024)
]
fig.add_trace(
    go.Scatter(
        x=rp_peaks["rp"],
        y=rp_peaks[val_col],
        text=rp_peaks["date"],
        textposition="top left",
        mode="markers+text",
        marker_color="red",
        textfont=dict(size=12, color="red"),
        marker_size=5,
    )
)

fig.update_layout(
    template="simple_white",
    xaxis=dict(dtick=1),
    width=800,
    height=600,
    title=f"{adm_name} - return period<br><sup>(as of {most_recent_date_str})</sup>",
    showlegend=False,
    margin={"t": 50, "l": 0, "r": 0, "b": 0},
)
fig.update_yaxes(title="Total population exposed to flooding during the year")
fig.update_xaxes(title="Return period (years)")
fig.show()
```

```python
peak_anytime_f = peak_anytime_f.sort_values("date")

fig = go.Figure()
fig.add_trace(
    go.Bar(
        x=peak_anytime_f["date"],
        y=peak_anytime_f[val_col],
        # mode="lines",
        # line_color="black",
    )
)
fig.update_layout(
    template="simple_white",
    xaxis=dict(dtick=1),
    width=800,
    height=600,
    title=f"{adm_name} - yearly timeseries<br><sup>(as of {most_recent_date_str})</sup>",
    showlegend=False,
    margin={"t": 50, "l": 0, "r": 0, "b": 0},
)
fig.update_yaxes(title="Total population exposed to flooding during the year")
fig.update_xaxes(title="Year")
fig.show()
```

```python
# current situation


def calculate_rp(group):
    group["rank"] = group[val_col].rank(ascending=False)
    group["rp"] = len(group) / group["rank"]
    return group
```

```python
bounds = [1, 2, 3, 5, 10, 20]
colors = [
    "whitesmoke",
    "lemonchiffon",
    "gold",
    "darkorange",
    "crimson",
    "rebeccapurple",
]

cmap = mcolors.ListedColormap(colors)
norm = mcolors.BoundaryNorm(bounds, cmap.N, extend="max")
```

```python
peak_anytime_adm2 = (
    peak_anytime.groupby("ADM2_PCODE")
    .apply(calculate_rp, include_groups=False)
    .reset_index(level=0)
)
```

```python
peak_anytime_adm1 = (
    peak_anytime.groupby(["ADM1_PCODE", "date"])[val_col]
    .sum()
    .reset_index()
    .groupby("ADM1_PCODE")
    .apply(calculate_rp, include_groups=False)
    .reset_index(level=0)
)
```

```python
gdf_plot = adm.merge(peak_anytime_adm2.set_index("date").loc[2024])
# cols = ["ADM1_FR", "ADM2_FR", "roll7", "rp"]
cols = ["ADM1_EN", "ADM2_EN", "roll7", "rp"]
(
    gdf_plot[cols]
    .sort_values("rp", ascending=False)
    .rename(columns={"roll7": "exposed"})
    .iloc[:50]
)
```

```python
fig, ax = plt.subplots(dpi=200)
gdf_plot.plot(
    column="rp",
    ax=ax,
    cmap=cmap,
    norm=norm,
    legend=True,
    legend_kwds={"label": "Return period (years)"},
)
# rp_label_thresh = 5
# for _, row in gdf_plot.iterrows():
#     if row["rp"] > 5:
#         centroid = row["geometry"].centroid
#         ax.annotate(
#             row["ADM2_FR"],
#             xy=(centroid.x, centroid.y),
#             fontsize=6,
#             color="black",
#             ha="center",
#             va="center",
#         )
gdf_plot.boundary.plot(linewidth=0.3, color="k", ax=ax)
ax.axis("off")
ax.set_title("Return period of peak 2024 flood exposure\nby Departement")
```

```python
gdf_plot = adm1.merge(peak_anytime_adm1.set_index("date").loc[2024])

# cols = ["ADM1_FR", "roll7", "rp"]
cols = ["ADM1_EN", "roll7", "rp"]
gdf_plot[cols].sort_values("rp", ascending=False).rename(
    columns={"roll7": "exposed"}
).iloc[:50]
```

```python
# name_col = "ADM1_FR"
name_col = "ADM1_EN"

fig, ax = plt.subplots(dpi=200, figsize=(10, 10))
gdf_plot.plot(
    column="rp",
    ax=ax,
    cmap=cmap,
    norm=norm,
    legend=True,
    legend_kwds={"label": "Return period (years)"},
)
for _, row in gdf_plot.iterrows():
    centroid = row["geometry"].centroid
    ax.annotate(
        row[name_col],
        xy=(centroid.x, centroid.y),
        fontsize=6,
        color="black",
        ha="center",
        va="center",
    )
gdf_plot.boundary.plot(linewidth=0.3, color="k", ax=ax)
ax.axis("off")
ax.set_title(
    f"Return period of peak 2024 flood exposure\nby Region, as of {most_recent_date_str}"
)
```

```python
adm
```

```python
# create pop tabular

dicts = []
for pcode, row in adm.set_index("ADM2_PCODE").iterrows():
    da_in = pop.rio.clip([row.geometry])
    dicts.append({"ADM2_PCODE": pcode, "total_population": int(da_in.sum())})
```

```python
df_total_pop = pd.DataFrame(dicts)
```

```python
df = df.merge(df_total_pop)
```

```python
df["frac_exposed"] = df["total_exposed"] / df["total_population"]
```

```python
df_max_year = (
    df.groupby(["ADM2_PCODE", df["date"].dt.year])[
        ["frac_exposed", "total_exposed"]
    ]
    .max()
    .reset_index()
)
```

```python
df_max_year
```

```python
df_avg_year = (
    df_max_year[df_max_year["date"] < 2024]
    .groupby("ADM2_PCODE")["total_exposed"]
    .mean()
    .reset_index()
    .rename(columns={"total_exposed": "total_exposed_mean"})
)
```

```python
df_avg_year
```

```python
df_max_year = df_max_year.merge(df_avg_year)
```

```python
df_max_year["total_exposed_anom"] = (
    df_max_year["total_exposed"] / df_max_year["total_exposed_mean"]
)
```

```python
df_max_year
```

```python
plot_year = 2024
fig, ax = plt.subplots(figsize=(8, 8), dpi=300)
adm.merge(df_max_year[df_max_year["date"] == plot_year]).plot(
    column="frac_exposed", ax=ax, cmap="Purples", legend=True
)
adm.boundary.plot(color="k", ax=ax, linewidth=0.2)
ax.axis("off")
ax.set_title(
    f"Peak fraction of popoulation exposed to flooding in {plot_year}\n(as of {most_recent_date_str})"
)
plt.show()
```

```python
plot_year = 2024
fig, ax = plt.subplots(figsize=(8, 8), dpi=300)
# Create a diverging colormap and center at 1
div_cmap = plt.get_cmap("coolwarm")  # Choose diverging colormap
norm = mcolors.TwoSlopeNorm(vmin=0, vcenter=1, vmax=3)  # Center at 1
adm.merge(df_max_year[df_max_year["date"] == plot_year]).plot(
    column="total_exposed_anom", ax=ax, cmap=div_cmap, norm=norm
)
adm.boundary.plot(color="k", ax=ax, linewidth=0.2)
ax.axis("off")
ax.set_title(
    f"Peak anomaly of popoulation exposed to flooding in {plot_year}\n(as of {most_recent_date_str})"
)
sm = plt.cm.ScalarMappable(cmap=div_cmap, norm=norm)
cbar = fig.colorbar(
    sm, ax=ax, extend="max"
)  # 'both' for upper and lower exceedance

# Format the colorbar as a percentage
cbar.ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1))
plt.show()
```
