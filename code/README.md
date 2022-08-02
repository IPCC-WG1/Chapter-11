# Code Readme

This folder contains the python code used for the data processing, analysis and visualization.

## Visualization

The jupyter notebooks (`*.ipynb`) are responsible for the plotting and visualization.
The main [README](../README.md) lists which notebook is used for which figure.

## Data Processing

Data processing and helper functions are organized in python modules packages (see _Code organization_). Of course this also relies on a number of external libraries (see _Conda environment_).

### Processing of CMIP data

CMIP6 (and CMIP5) data is processed from the `process.py` file.


```bash
ipython process.py tas_globmean
ipython process.py tas_annmean
ipython process.py tas_monthly
ipython process.py tas_summer_months
ipython process.py pr_annmean
ipython process.py pr_monthly
ipython process.py txx
ipython process.py txx_monthly
ipython process.py tnn
ipython process.py tnn_monthly
ipython process.py rx1day
ipython process.py rx1day_monthly
ipython process.py rx5day
ipython process.py cdd
ipython process.py mrso
ipython process.py mrso_annmean
ipython process.py mrso_annmean_CMIP5
ipython process.py mrsos
ipython process.py mrsos_annmean
```


## Conda environment

This repository was used inside a conda environment using python 3.7. The used packages are listed in [../../environment.yml](../../environment.yml).

## Code organization

- `filefinder`: List, create, and parse file and directory content; see below
- `fixes`: fixes applied to the unprocessed cmip5 and cmip6 data
-  `utils`: helper functions, e.g. for ploting
- `utils/transform`: Functions to process an `xr.Dataset`, e.g. calculate annual maxima, global mean, resample, or regional means. These functions are independent of cmip5 or cmip6.
- `utils/postprocess`: Wraps the functions in `utils/transform` for cmip5 and cmip6 data.

---

# Internals

The rest of this file adds some notes on the internals of the code in this repo. It's far from complete and not necessarily meant for public consumption.


## Terminology

I use some terms and classes that are not obvious which are explained below, however, this list does not claim to be comprehensive.

#### FileFinder

The _FileFinder_ is used to define regular folder and file patterns with the python string format syntax. It can parse (regular) folder and file names. See [filefinder/README.md](filefinder/README.md) for details.

### DataList

_DataList_ is a data structure inspired by [ESMValTool](https://github.com/ESMValGroup/ESMValTool/). It is used as a flat data container to store model data (e.g. `xr.DataArray` objects) and its metadata. It is a list of data, metadata tuples:
```python
datalist = [
    (ds, meta),
    (ds, meta),
    ...
]
```
where `ds` is a data store (e.g. an `xr.DataArray`) and meta is a `dict` containing metadata, e.g. `meta = {"model": "CESM", "exp": "ssp585", ...}`. There are functions to handle this data structure in utils/computation.py, e.g.:

```python
computation.select_by_metadata(datalist, exp="ssp585")
computation.process_datalist(datalist, compute_time_average, period=slice(1850, 1900))
```

This allows to (1) work with a convenient flat data structure (no nested loops), (2) store arbitrary data (e.g. `xr.DataArray` objects with different grids), (3) carry around metadata without having to alter the data store.


##  Ideas for code improvement

I list some suggestions how the code base could be improved.

### FileFinder

_FileFinder_ could be extracted to its own package. Now available under [github.com/mathause/filefinder](https://github.com/mathause/filefinder/).


### DataList

The _DataList_ structure could be turned into a class. This would allow a much nicer syntax. E.g. the above examples could be written as:
```python
datalist.select(exp="ssp585")
datalist.map(compute_time_average, period=slice(1850, 1900))
```

The API could look something like this:

```python
class DataList:

    def __init__(self, data: List[Any], meta: List[dict]):

        assert len(data) == len(meta)
        ...

    def select(self, **attributes):
      """Select specific data described by metadata.

      would replace computation.select_by_metadata
      """

    def map(self, func, pass_meta=False, **kwargs):
      """loop over DataList and apply a function

      would replace computation.process_datalist"""

    def to_dataarray(self, along):
      """concatenate data along a dimension and return as xr.DataArray

      would replace computation.concat_xarray_with_metadata
      """


def join(*objects, select_by=("model", "exp", "ens"), check=True):
    """align DataList objects (inner join)

    would replace computation.match_data_list
    """
```

### CMIP data processing

- The setup of the CMIP data processing is powerful but too complicated. It currently needs at least two classes (a `Transformer` and a `Processor`) and a setup in `process.py`. It is thus difficult to add a new way to process the data. This should be simplified considerably (but I am not sure how).
- Abstracting away the difference between `FromOrig` (from the raw cmip archive) and `FromPost` (a second post process step) could also help to make it simpler.
- Would be nice if each `Processor` had it's own name that it automatically prefixes, such that the name does not have to be built manually.
- Loading masks and weights (e.g. `get_lat_weights`, `get_land_mask`) is currently done in the wrong class. It should be moved from `Processor` to the `conf.cmip` class.
- Rename arguments `metadata` to `meta` for all functions. Rather pass `meta` as `dict` and not as keyword arguments (or `**kwargs`) - in this way we can directly pass all of them through.
- If a simulation cannot be processed because e.g. the grid weights are missing this is indicated with an empty list and then I use number of if statements. This could potentially be done with errors - e.g. `WeightsNotFoundError` - this may remove the need for the if statements and attach a reason for the failure (but would require a dedicated error handler).
- Simulations that are removed (but not fixed) from postprocessing in `fixes._fixes_cmip?.cmip6_files` should not turn of in the list of simulations to process.

### utils/plot.py

The plotting helper routines are organized as functions. This has turned out to be sub-optimal, e.g. not all handles can be returned (as there are too many: `ax`, `h`, `cbar`, `legend_handle`, ...) and a function is needed for combinations of plots (`map`, `map_hatched`, `map_hatched_simple`, ...). Organizing this as a class could help to tidy this up, e.g. handles can be properties, and the methods could be separated (`map`, `hatch`).
