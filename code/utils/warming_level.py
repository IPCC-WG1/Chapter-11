from .datalist import concat_xarray_with_metadata, select_by_metadata


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

    if not isinstance(n_years, int) or n_years < 1:
        raise ValueError(f"n_years must be a positive integer, got {n_years}")

    if n_years % 2 != 0:  # odd
        beg_offset = end_offset = (n_years - 1) // 2
    else:  # even
        beg_offset = n_years // 2
        end_offset = n_years // 2 - 1

    anomalies = anomalies.rolling(year=n_years, center=True).mean()

    # find years warmer than 'warming_level'
    sel = anomalies - warming_level > 0.0

    # if no warmer year is found, return
    if not sel.any():
        return None, None, None

    # find index of central year
    idx = sel.argmax().values

    central_year = anomalies.isel(year=idx).year.values

    beg = int(central_year - beg_offset)
    end = int(central_year + end_offset)

    return beg, end, central_year


def at_warming_level(
    tas_list,
    index_list,
    warming_level,
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

    return concat_xarray_with_metadata(out)


def at_warming_levels_list(
    tas_list,
    index_list,
    warming_levels,
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
            reduce=reduce,
            select_by=select_by,
            skipna=skipna,
            as_datalist=as_datalist,
            n_years=n_years,
            **kwargs,
        )

        if factor is not None:
            if as_datalist:
                raise ValueError("That does not work")
            res *= factor

        out[str(warming_level)] = res

    return out


# def calc_anomaly_wrt_warming_level(
#     tas_list,
#     index_list,
#     warming_level,
#     how="absolute",
#     skipna=None,
#     select_by=("model", "exp", "ens"),
# ):
#     """calc anomaly of dataset w.r.t. a warming level

#     Parameters
#     ----------
#     tas_list : DataList
#         DataList of global mean temperatures.
#     index_list : DataList
#         DataList of the index to calculate the anomaly for.
#     warming_level : float
#         Global warming level (GWL) to compute the anomaly for.
#     skipna : bool, default: None
#         If invalid values should be skipped.
#     how : "absolute" | "relative" | "norm" | "no_anom"
#         Method to calculate the anomaly. Default "absolute". Prepend "no_check_" to
#         avoid the time bounds check.
#     select_by : list of str, optional
#         Conditions to align tas_list and index_list.
#     """

#     out = list()

#     # loop through all global mean temperatures
#     for tas, metadata in tas_list:
#         attributes = {key: metadata[key] for key in select_by}

#         # try to find the index
#         index = select_by_metadata(index_list, **attributes)

#         # make sure only one dataset is found in index_list
#         if len(index) > 1:
#             raise ValueError("Found more than one dataset:\n", metadata)

#         # an index was found for this tas dataset
#         if index:

#             # determine year when the warming was first reached
#             beg, end, __ = calc_year_of_warming_level(tas.tas, warming_level)

#             if beg:

#                 index = calc_anomaly(
#                     index,
#                     beg,
#                     end,
#                     how=how,
#                     skipna=skipna,
#                     metadata=metadata,
#                     at_least_until=None,
#                 )

#                 out.append([index, metadata])

#     return out
