# Flood exposure monitoring

Using daily Floodscan flood extent and WorldPop population data to estimate
population exposed to flooding for countries in Africa.

## Usage

Runs daily as the Databricks job **Flood Exposure Monitoring** (23:15 UTC,
after the FloodScan COGs are updated by
[ds-raster-pipelines](https://github.com/OCHA-DAP/ds-raster-pipelines)),
defined in `databricks.yml` — one job with three chained tasks:
exposure rasters → raster stats → quantiles. `STAGE` (the ocha-stratus data
plane) is a bundle variable: the `prod` target sets `prod`, the `dev` target
`dev`.

```shell
databricks bundle validate -t prod -p DEFAULT
databricks bundle deploy   -t prod -p DEFAULT        # (re)deploy the live job
databricks bundle run flood_exposure_monitoring -t prod -p DEFAULT
databricks bundle run init_iso3 -t prod -p DEFAULT --params iso3=<new-iso3-code>
```

The GitHub Actions workflows remain as a manual fallback
(`workflow_dispatch` on "Update exposure rasters" runs the whole chain).

To run locally, set the environment variables `DSCI_AZ_BLOB_DEV_SAS_WRITE`,
`DSCI_AZ_BLOB_PROD_SAS_WRITE`, `DSCI_AZ_DB_DEV_PW_WRITE`,
`DSCI_AZ_DB_PROD_PW_WRITE`, `DSCI_AZ_DB_PROD_UID_WRITE`, `DSCI_AZ_DB_DEV_UID_WRITE`
and set up a virtual environment and install the requirements:

```shell
pip install -r requirements.txt
pip install -e .
```

Then run the pipeline with:

```shell
python pipelines/update_exposure.py
python pipelines/update_raster_stats.py
python pipelines/update_exposure_quantile.py
```

### To add data for a new ISO3 code

1. Add the code to the list of ISO3s in `src.constants.py`,
commit the changes, and open a new PR

2. Initialize the required datasets by running

```shell
python pipelines/init_iso3.py --iso3 <new-iso3-code>
````

or initialize all available ISO3 codes by running

```shell
python pipelines/init_iso3.py --iso3 all
```

## Structure

```plaintext
.
├── .github/
│   └── ...                    # GH Action workflow
├── exploration/
│   └── ...                    # notebooks for exploration
├── pipelines/
│   ├── update_exposure.py     # script for updating exposure rasters
│   └── update_raster_stats.py # script for updating exposure raster stats
└── src/
    ├── datasources/
    │   ├── codab.py           # downloading and loading CODABs
    │   ├── floodscan.py       # functions to calculate exposure, load Floodscan
    │   └── worldpop.py        # load and download Worldpop population rasters
    ├── utils/
    │   ├── blob.py            # read and write for Azure blob storage
    │   ├── database.py        # read and write to Postgres DB
    │   └── raster.py          # just function to upsample rasters
    └── constants.py           # constants
```

## Development

All code is formatted according to black and flake8 guidelines.
The repo is set-up to use pre-commit.
Before you start developing in this repository, you will need to run

```shell
pre-commit install
```

The `markdownlint` hook will require
[Ruby](https://www.ruby-lang.org/en/documentation/installation/)
to be installed on your computer.

You can run all hooks against all your files using

```shell
pre-commit run --all-files
```

It is also **strongly** recommended to use `jupytext`
to convert all Jupyter notebooks (`.ipynb`) to Markdown files (`.md`)
before committing them into version control. This will make for
cleaner diffs (and thus easier code reviews) and will ensure that cell outputs aren't
committed to the repo (which might be problematic if working with sensitive data).
