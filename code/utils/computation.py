import xarray as xr


def time_in_range(start, end, yr_min, yr_max, metadata, quiet=False):
    """determine if start--end is in time vector"""

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
    ds, start, end, how="absolute", skipna=None, metadata=None, at_least_until=None, quiet=False
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
    """

    check_time_bounds = True
    if how.startswith("no_check_"):
        check_time_bounds = False
        how = how.replace("no_check_", "")

    assert how in ("absolute", "relative", "norm", "no_anom")

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
    if check_time_bounds and not time_in_range(
        int(start), int(end), yr_min, yr_max, metadata=metadata, quiet=quiet
    ):
        return []

    if at_least_until is not None and not time_in_range(
        int(at_least_until), int(at_least_until), yr_min, yr_max, metadata=metadata, quiet=quiet
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
    """Select specific metadata describing preprocessed data.
    Parameters
    ----------
    metadata : list of (ds, metadata) pairs
        A list of metadata describing preprocessed data.
    **attributes :
        Keyword arguments specifying the required variable attributes and
        their values.
        Use the value '*' to select any variable that has the attribute.

    Returns
    -------
    list of (ds, metadata) pairs
        A list of matching metadata.
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
    """Select specific metadata describing preprocessed data.
    Parameters
    ----------
    metadata : list of (ds, metadata) pairs
        A list of metadata describing preprocessed data.
    **attributes :
        Keyword arguments specifying the required variable attributes and
        their values.
        Use the value '*' to select any variable that has the attribute.

    Returns
    -------
    list of (ds, metadata) pairs
        A list of matching metadata.
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
    """compute value of index at a several warming levels

    Parameters
    ==========
    tas_list : list of (ds, metadata) pairs
        List of (ds, metadata) pairs containing annual mean global mean
        temperature data.
    index_list : list of (ds, metadata) pairs
        List of (ds, metadata) pairs containing annual data of the index.
    warming_levels : iterable of float
        warming levels at which to assess the index
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
    """compute value of index at a several warming levels

    Parameters
    ==========
    tas_list : list of (ds, metadata) pairs
        List of (ds, metadata) pairs containing annual mean global mean
        temperature data.
    index_list : list of (ds, metadata) pairs
        List of (ds, metadata) pairs containing annual data of the index.
    warming_levels : iterable of float
        warming levels at which to assess the index
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
    """compute value of index at a certain warming level

    Parameters
    ==========
    tas_list : list of (ds, metadata) pairs
        List of (ds, metadata) pairs containing annual mean global mean
        temperature data.
    index_list : list of (ds, metadata) pairs
        List of (ds, metadata) pairs containing annual data of the index.
    warming_level : float
        warming level at which to assess the index
    select_by : iterable of str
        List attributes on which to select from index_list.
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


def time_average(index_list, beg, end, reduce="mean", skipna=None, as_datalist=False, **kwargs):
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
    """align DataArrays by model"""

    return align_modellist(data, join="inner", by=by)


def align_modellist(data, join="inner", by=dict(ens=("model", "ensname", "exp"))):

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
    data : nested dict


    """

    all_ds = list()

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

    # def all_none(lst):
    #     return np.vectorize(lambda x: x is None)(lst).all()
    # retain_dict = {key: val for key, val in retain_dict.items() if not all_none(val)}

    out = out.assign_coords(**retain_dict)

    index = {"mod_ens": retain}

    if set_index:
        # create multiindex
        out = out.set_index(**index)

    return out


def concat_xarray_without_metadata(datalist, process=None):
    """create xr Dataset with 'ens' and 'model' as multiindex

    Input
    -----
    datalist : datalist

    """

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
    datalist : datalist
        List to apply the function over.
    **kwargs : extra arguments
        passed to func
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
