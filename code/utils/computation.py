import warnings

import xarray as xr

# === NOTE ===
# restructured the content of this file after the analysis. Import all the functions
# here so the code still works.

from .datalist import (  # noqa: F401
    concat_xarray_with_metadata,
    match_data_list,
    process_datalist,
    remove_by_metadata,
    select_by_metadata,
)
from .warming_level import (  # noqa: F401
    at_warming_level,
    at_warming_levels_dict,
    at_warming_levels_list,
    calc_year_of_warming_level,
)


def _time_in_range(start, end, yr_min, yr_max, meta, quiet=False):
    """determine if start--end is in time vector

    Parameters
    ----------
    start : int
        Start year.
    ens : int
        End year.
    yr_min : int
        First year on time vector.
    yr_max : int
        Last year on time vector.
    meta : dict
        metadata of the model.
    quiet : bool, default: False
        If a warning should be printed.

    Returns
    -------
    _time_in_range : bool
        True if time is in range, else False.
    """

    if (start < yr_min) or (end > yr_max):
        msg = f"no data for {start} - {end} ({yr_min.values}..{yr_max.values})"

        if meta is not None:

            meta = meta.copy()

            # get rid of the ens labels
            meta.pop("r", None)
            meta.pop("i", None)
            meta.pop("p", None)
            meta.pop("f", None)

            msg = f" -- {meta}: " + msg

        if not quiet:
            print(msg)

        return False
    else:
        return True


def calc_anomaly(
    ds,
    start,
    end,
    how="absolute",
    skipna=None,
    meta=None,
    at_least_until=None,
    quiet=False,
):
    """calc anomaly of dataset

    Parameters
    ----------
    ds : xarray Dataset or DataArray
        Data that needs to be normalized
    start : int
        Start year of the reference period.
    end : int
        End year of the reference period.
    how : "absolute" | "relative" | "norm" | "no_anom"
        Method to calculate the anomaly. Default "absolute". Prepend "no_check_" to
        avoid the time bounds check.
    skipna : bool, default: None
        If invalid values should be skipped.
    meta : metadata, optional
        Used to display a message if the models fails the bounds check
    at_least_until : int, default: None
        If not None ensure ds runs at least to this year.
    quiet : bool, default: True
        If check_time_bounds should print a message on failure.

    Returns
    -------
    ds : xarray Dataset or DataArray or empty list
        Normalized data

    """

    check_time_bounds = True
    if how.startswith("no_check_"):
        check_time_bounds = False
        how = how.replace("no_check_", "")

    how_options = ("absolute", "relative", "norm", "no_anom")
    if how not in how_options:
        raise ValueError("'how' must be one of: " + ",".join(how_options))

    if ("year" in ds.dims) and ("time" in ds.dims):
        msg = "'year' and 'time' in dims"
        raise KeyError(msg)

    # if annual values have been calculated with groupby
    # (groupby('time.year').mean('year'))
    if "year" in ds.dims:
        years = ds.year
        dim = "year"
    else:
        years = ds.time.dt.year
        start, end = str(start), str(end)
        dim = "time"

    # check if time series spans reference period
    yr_min, yr_max = years.min(), years.max()

    if check_time_bounds and not _time_in_range(
        int(start), int(end), yr_min, yr_max, meta=meta, quiet=quiet
    ):
        return []

    if at_least_until is not None and not _time_in_range(
        int(at_least_until),
        int(at_least_until),
        yr_min,
        yr_max,
        meta=meta,
        quiet=quiet,
    ):
        return []

    selector = {dim: slice(start, end)}

    # require at least one year of data even when doing no check
    if not check_time_bounds and len(ds.sel(**selector)[dim]) == 0:
        return []

    if how != "no_anom":
        mean = ds.sel(**selector).mean(dim, skipna=skipna)

    if how == "norm":
        std = ds.sel(**selector).std(dim, skipna=skipna)

    if how == "no_anom":
        return ds
    elif how == "absolute":
        return ds - mean
    elif how == "relative":
        return (ds - mean) / mean * 100
    elif how == "norm":
        return (ds - mean) / std


def time_average(
    datalist, beg, end, reduce="mean", skipna=None, as_datalist=False, **kwargs
):
    """compute time average of index

    Parameters
    ==========
    datalist : DataList
        List of (ds, metadata) pairs containing annual data of the index.
    beg : int
        Start year to calculate the average over.
    end : int
        End year to calculate the average over.
    warming_levels : iterable of float
        warming levels at which to assess the index
    reduce : str or None, default: "mean"
        How to compute the average over the warming level period. If None the individual
        years are returned.
    skipna : bool, default: None
        If True, skip missing values (as marked by NaN).
    as_datalist, bool, default: False
        If True returns data as DataList else as xr.DataArray.
    kwargs : dict
        Additional keyword arguments passed on to the average function.

    Returns
    -------
    out : xr.DataArray or DataList
        Data averaged over the given time period. Output type depends on ``as_datalist``.
    """

    def _inner(ds, meta, beg, end, reduce, skipna):

        da = ds[meta["varn"]]

        da = da.sel(year=slice(beg, end))

        if reduce is not None:
            # calculate mean
            da = getattr(da, reduce)("year", skipna=skipna, **kwargs)

        return da

    datalist = process_datalist(
        _inner,
        datalist,
        pass_meta=True,
        beg=beg,
        end=end,
        reduce=reduce,
        skipna=skipna,
    )

    if as_datalist:
        return datalist

    return concat_xarray_with_metadata(
        datalist,
        set_index=False,
    )


def align_modellist(data, join="inner", by=dict(ens=("model", "ensname", "exp"))):
    """align DataArrays

    Parameters
    ----------
    data : iterable of xr.DataArray
        DataArray objects to align
    by : list of str, optional
        Conditions to align lists on.

    Returns
    -------
    out : list of xr.DataArray
        List of aligned DataArray objects.

    """

    warnings.warn("Maybe better not use this")

    res = list()
    for i, o in enumerate(data):
        # create a MultiIndex
        res.append(o.set_index(**by))

    return list(xr.align(*res, join=join))


def select_same_models(data, by=dict(ens=("model", "ensname", "exp"))):
    """select same models by aligning DataArrays

    Parameters
    ----------
    data : iterable of xr.DataArray
        DataArray objects to align
    by : list of str, optional
        Conditions to align lists on.

    Returns
    -------
    out : list of xr.DataArray
        List of aligned DataArray objects.

    """

    return align_modellist(data, join="inner", by=by)
