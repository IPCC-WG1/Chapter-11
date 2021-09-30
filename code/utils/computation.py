import warnings

import xarray as xr


def _time_in_range(start, end, yr_min, yr_max, metadata, quiet=False):
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
    metadata : dict
        MetaData of the model.
    quiet : bool, default: False
        If a warning should be printed.

    Returns
    -------
    _time_in_range : bool
        True if time is in range, else False.
    """

    if (start < yr_min) or (end > yr_max):
        msg = f"no data for {start} - {end} ({yr_min.values}..{yr_max.values})"

        if metadata is not None:

            metadata = metadata.copy()

            # get rid of the ens labels
            metadata.pop("r", None)
            metadata.pop("i", None)
            metadata.pop("p", None)
            metadata.pop("f", None)

            msg = f" -- {metadata}: " + msg

        if not quiet:
            print(msg)

        return False
    else:
        return True


def calc_anomaly_wrt_warming_level(
    tas_list,
    index_list,
    warming_level,
    how="absolute",
    skipna=None,
    select_by=("model", "exp", "ens"),
):
    """calc anomaly of dataset w.r.t. a warming level

    Parameters
    ----------
    tas_list : DataList
        DataList of global mean temperatures.
    index_list : DataList
        DataList of the index to calculate the anomaly for.
    warming_level : float
        Global warming level (GWL) to compute the anomaly for.
    skipna : bool, default: None
        If invalid values should be skipped.
    how : "absolute" | "relative" | "norm" | "no_anom"
        Method to calculate the anomaly. Default "absolute". Prepend "no_check_" to
        avoid the time bounds check.
    select_by : list of str, optional
        Conditions to align tas_list and index_list.
    """

    out = list()

    # loop through all global mean temperatures
    for tas, metadata in tas_list:
        attributes = {key: metadata[key] for key in select_by}

        # try to find the index
        index = select_by_metadata(index_list, **attributes)

        # make sure only one dataset is found in index_list
        if len(index) > 1:
            raise ValueError("Found more than one dataset:\n", metadata)

        # an index was found for this tas dataset
        if index:

            # determine year when the warming was first reached
            beg, end, __ = calc_year_of_warming_level(tas.tas, warming_level)

            if beg:

                index = calc_anomaly(
                    index,
                    beg,
                    end,
                    how=how,
                    skipna=skipna,
                    metadata=metadata,
                    at_least_until=None,
                )

                out.append([index, metadata])

    return out


def calc_anomaly(
    ds,
    start,
    end,
    how="absolute",
    skipna=None,
    metadata=None,
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
    metadata : MetaData, optional
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
        raise ValueError("'how' must be one of: " + ",", join(how_options))

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
        int(start), int(end), yr_min, yr_max, metadata=metadata, quiet=quiet
    ):
        return []

    if at_least_until is not None and not _time_in_range(
        int(at_least_until),
        int(at_least_until),
        yr_min,
        yr_max,
        metadata=metadata,
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


def calc_year_of_warming_level(anomalies, warming_level, n_years=20):
    """calculate the period when a certain global warming level is reached

    Parameters
    ----------
    anomalies : xr.DataArray
        Global mean temperature anomalies.
    warming_level : float
        Global warming level
    n_years : int, default: 20
        Length of period over which global warming level must be reached. Currently
        restricted to even number of years.

    Returns
    -------
    beg: int
        Start year of the period. None if warming level is not exceeded.
    end: int
        End year of the period. None if warming level is not exceeded.
    central_year: int
        central year of the period. None if warming level is not exceeded.

    """

    # calculate the start and end year of period of first exceedance

    if n_years % 2 != 0:
        raise ValueError(f"n_years must be an even integer, found {n_years}")

    anomalies = anomalies.rolling(year=n_years, center=True).mean()

    # find years warmer than 'warming_level'
    sel = anomalies - warming_level > 0.0

    # if no warmer year is found, return
    if not sel.any():
        return None, None, None

    # find index of central year
    idx = sel.argmax().values

    central_year = anomalies.isel(year=idx).year.values

    beg = int(central_year - n_years / 2)
    end = int(central_year + (n_years / 2 - 1))

    return beg, end, central_year


def select_by_metadata(datalist, **attributes):
    """Select specific data described by metadata.

    Parameters
    ----------
    datalist : DataList
        List of (ds, metadata) pairs.
    **attributes :
        Keyword arguments specifying the required variable attributes and their values.
        Use '*' to select any variable that has the attribute.

    Returns
    -------
    out: DataList
        List of (ds, metadata) pairs that has matching metadata.
    """

    selection = []
    for data, metadata in datalist:

        if all(
            a in metadata and (metadata[a] == attributes[a] or attributes[a] == "*")
            for a in attributes
        ):
            selection.append((data, metadata))
    return selection


def remove_by_metadata(datalist, **attributes):
    """Remove specific data described by metadata.

    Parameters
    ----------
    datalist : DataList
        List of (ds, metadata) pairs
    **attributes :
        Keyword arguments specifying the required variable attributes and
        their values.

    Returns
    -------
    out : DataList
        List of (ds, metadata) pairs with non-matching metadata.
    """

    selection = []
    for data, metadata in datalist:

        if all(
            a in metadata and (metadata[a] == attributes[a] or attributes[a] == "*")
            for a in attributes
        ):
            pass
        else:
            selection.append((data, metadata))
    return selection


def at_warming_levels_list(
    tas_list,
    index_list,
    warming_levels,
    add_meta=True,
    reduce="mean",
    select_by=("model", "exp", "ens"),
    factor=None,
    skipna=None,
    as_datalist=False,
    n_years=20,
):
    """compute value of index at several warming levels, returned in a list

    Parameters
    ==========
    tas_list : DataList
        List of (ds, metadata) pairs containing annual mean global mean temperature data
    index_list : DataList
        List of (ds, metadata) pairs containing annual data of the index.
    warming_levels : iterable of float
        warming levels at which to assess the index
    add_meta : bool: default: True
        If metadata should be added when returning a xr.DataArray.
    reduce : str or None, default: "mean"
        How to compute the average over the warming level period. If None the individual
        years are returned.
    select_by : list of str, optional
        Conditions to align tas_list and index_list.
    factor : float, default: None
        If givem multiplies the data in index_list with this factor.
    skipna : bool, default: None
        If True, skip missing values (as marked by NaN).
    as_datalist, bool, default: False
        If True returns data as DataList else as xr.DataArray.
    n_years : int, default: 20
        Length of period over which global warming level must be reached. Currently
        restricted to even number of years.

    Returns
    -------
    out : list of xr.DataArray or list DataList
        Data at the given warming levels. Output type depends on ``as_datalist``.
    """

    out = list()

    for warming_level in warming_levels:
        res = at_warming_level(
            tas_list,
            index_list,
            warming_level,
            add_meta=add_meta,
            reduce=reduce,
            select_by=select_by,
            skipna=skipna,
            as_datalist=as_datalist,
            n_years=n_years,
        )

        if factor is not None:
            if as_datalist:
                raise ValueError("Cannot set `factor` and `as_datalist`")
            res *= factor

        out.append(res)

    return out


def at_warming_levels_dict(
    tas_list,
    index_list,
    warming_levels,
    add_meta=True,
    reduce="mean",
    select_by=("model", "exp", "ens"),
    factor=None,
    skipna=None,
    as_datalist=False,
    n_years=20,
    **kwargs,
):
    """compute value of index at several warming levels, returned in a dict

    Parameters
    ==========
    tas_list : DataList
        List of (ds, metadata) pairs containing annual mean global mean temperature data
    index_list : DataList
        List of (ds, metadata) pairs containing annual data of the index.
    warming_levels : iterable of float
        warming levels at which to assess the index
    add_meta : bool: default: True
        If metadata should be added when returning a xr.DataArray.
    reduce : str or None, default: "mean"
        How to compute the average over the warming level period. If None the individual
        years are returned.
    select_by : list of str, optional
        Conditions to align tas_list and index_list.
    factor : float, default: None
        If givem multiplies the data in index_list with this factor.
    skipna : bool, default: None
        If True, skip missing values (as marked by NaN).
    as_datalist, bool, default: False
        If True returns data as DataList else as xr.DataArray.
    n_years : int, default: 20
        Length of period over which global warming level must be reached. Currently
        restricted to even number of years.
    kwargs : dict
        Additional keyword arguments passed on to the average function.

    Returns
    -------
    out : dict of xr.DataArray or dict of DataList
        Data at the given warming levels. The given warming_levels are the dict's keys.
        Output type depends on ``as_datalist``.
    """

    out = dict()

    for warming_level in warming_levels:
        res = at_warming_level(
            tas_list,
            index_list,
            warming_level,
            add_meta=add_meta,
            reduce=reduce,
            select_by=select_by,
            skipna=skipna,
            as_datalist=as_datalist,
            n_years=n_years,
            **kwargs,
        )

        if factor is not None:
            if as_datalist:
                raise ValueError("Cannot set `factor` and `as_datalist`")
            res *= factor

        out[str(warming_level)] = res

    return out


def at_warming_level(
    tas_list,
    index_list,
    warming_level,
    add_meta=True,
    reduce="mean",
    select_by=("model", "exp", "ens"),
    skipna=None,
    as_datalist=False,
    n_years=20,
    **kwargs,
):
    """compute value of index at one warming level

    Parameters
    ==========
    tas_list : DataList
        List of (ds, metadata) pairs containing annual mean global mean temperature data
    index_list : DataList
        List of (ds, metadata) pairs containing annual data of the index.
    warming_level : float
        warming level at which to assess the index
    add_meta : bool: default: True
        If metadata should be added when returning a xr.DataArray.
    reduce : str or None, default: "mean"
        How to compute the average over the warming level period. If None the individual
        years are returned.
    select_by : list of str, optional
        Conditions to align tas_list and index_list.
    skipna : bool, default: None
        If True, skip missing values (as marked by NaN).
    as_datalist, bool, default: False
        If True returns data as DataList else as xr.DataArray.
    n_years : int, default: 20
        Length of period over which global warming level must be reached. Currently
        restricted to even number of years.
    kwargs : dict
        Additional keyword arguments passed on to the average function.

    Returns
    -------
    out : xr.DataArray or DataList
        Data at the given warming level. Output type depends on ``as_datalist``.
    """

    out = list()

    # loop through all global mean temperatures
    for tas, metadata in tas_list:

        attributes = {key: metadata[key] for key in select_by}

        # try to find the index
        index = select_by_metadata(index_list, **attributes)

        # make sure only one dataset is found in index_list
        if len(index) > 1:
            raise ValueError("Found more than one dataset:\n", metadata)

        # an index was found for this tas dataset
        if index:

            # determine year when the warming was first reached
            beg, end, center = calc_year_of_warming_level(
                tas.tas, warming_level, n_years=n_years
            )

            if beg:
                ds_idx = index[0][0]
                metadata_idx = index[0][1]

                # get the Dataarray
                da_idx = ds_idx[metadata_idx["varn"]]
                idx = da_idx.sel(year=slice(beg, end))

                if reduce is not None:
                    # calculate mean
                    idx = getattr(idx, reduce)("year", skipna=skipna, **kwargs)
                else:
                    # drop year to enable concatenating
                    idx = idx.drop_vars("year")

                out.append([idx, metadata_idx])

    if not out:
        return []

    if as_datalist:
        return out

    if add_meta:
        return concat_xarray_with_metadata(out)
    else:
        return concat_xarray_without_metadata(out)


def time_average(
    index_list, beg, end, reduce="mean", skipna=None, as_datalist=False, **kwargs
):
    """compute time average of index

    Parameters
    ==========
    index_list : DataList
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

    index_list = process_datalist(
        _inner,
        index_list,
        pass_meta=True,
        beg=beg,
        end=end,
        reduce=reduce,
        skipna=skipna,
    )

    if as_datalist:
        return index_list

    return concat_xarray_with_metadata(
        index_list,
        set_index=False,
    )


def match_data_list(list_a, list_b, select_by=("model", "exp", "ens"), check=True):

    """align two datalists (inner join)

    Parameters
    ----------
    list_a : DataList
        List of (ds, metadata) pairs.
    list_b : DataList
        List of (ds, metadata) pairs.
    select_by : list of str, optional
        Conditions to align lists on.
    check : bool, default: True
        If True checks that only one dataset is found in list_b

    Returns
    -------
    out_a : DataList
        Aligned list of (ds, metadata) pairs.
    out_b : DataList
        Aligned list of (ds, metadata) pairs.
    """

    out_a = list()
    out_b = list()

    for ds_a, metadata in list_a:

        attributes = {key: metadata[key] for key in select_by}

        # try to find the in list_b
        match = select_by_metadata(list_b, **attributes)

        # make sure only one dataset is found in index_list
        if check and len(match) > 1:
            print(match)
            raise ValueError(metadata)

        # an index was found for this dataset
        if match:
            out_a += [[ds_a, metadata]]
            out_b += match

    return out_a, out_b


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


def concat_xarray_with_metadata(
    datalist,
    process=None,
    index={"mod_ens": ("model", "ens")},
    retain=("model", "ens", "ensnumber", "exp", "postprocess", "table", "grid", "varn"),
    set_index=False,
):
    """create xr Dataset with 'ens' and 'model' as multiindex

    Input
    -----
    datalist : DataList
        List of (ds, metadata) pairs.
    process : callable, default: None
        Function to apply for each ds before concatenation.
    index : dict, optional
        Only applies if set_index is True. dict describing how to create a MultiIndex.
    retain : iterable of str, optional
        Which metadata information to assign as non-dimension coordinates.
    set_index : bool, default: False
        If True sets a MultiIndex to the DataArray

    Returns
    -------
    out : xr.DataArray
        Concatenated DataArray


    Notes
    -----
    should be named ``to_dataarray``

    """

    all_ds = list()

    # no longer necessary since we can plot non-coord dimensions
    retain += ("ensi",)
    retain_dict = {r: ("mod_ens", list()) for r in retain}

    for i, (ds, metadata) in enumerate(datalist):

        if process is not None:
            ds = process(ds)

        all_ds.append(ds)

        retain_dict["ensi"][1].append(i)
        for r in retain[:-1]:
            retain_dict[r][1].append(metadata.get(r, None))

    # concate all data
    out = xr.concat(all_ds, "mod_ens", compat="override", coords="minimal")
    # assign coordinates
    out = out.assign_coords(**retain_dict)

    if set_index:
        index = {"mod_ens": retain}
        # create multiindex
        out = out.set_index(**index)

    return out


def concat_xarray_without_metadata(datalist, process=None):
    """create xr Dataset with 'ens' and 'model' as multiindex

    Input
    -----
    datalist : datalist

    """

    warnings.warn("Maybe better not use this")

    all_ds = list()

    for i, (ds, metadata) in enumerate(datalist):

        if process is not None:
            ds = process(ds)

        all_ds.append(ds)

    # concate all data
    out = xr.concat(all_ds, "ens", compat="override", coords="minimal")

    return out


def process_datalist(func, datalist, pass_meta=False, **kwargs):
    """loop over a datalist and apply a function

    Parameters
    ----------
    func : callable
        function to apply
    datalist : DataList
        List to apply the function over.
    pass_meta : bool, default: False
        If "meta" should be passed as keyword argument to ``func``.
    **kwargs : extra arguments
        passed to func

    Returns
    -------
    datalist_out : DataList
        List with ``func`` applied to each element.
    """

    datalist_out = list()

    for ds, meta in datalist:

        if pass_meta:
            ds = func(ds, meta=meta, **kwargs)
        else:
            ds = func(ds, **kwargs)

        if len(ds) == 0:
            continue

        datalist_out.append([ds, meta])

    return datalist_out
