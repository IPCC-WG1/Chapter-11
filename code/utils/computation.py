import xarray as xr


def time_in_range(start, end, yr_min, yr_max, metadata):
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

        print(msg)

        return False
    else:
        return True


def calc_anomaly(ds, start, end, how="absolute", skipna=None, metadata=None):
    """calc anomaly of dataset

        Parameters
        ----------
        ds : xarray Dataset or DataArray
            Data that needs to be normalized
        start : integer
            Start year of the reference period.
        end : integer
            End year of the reference period.
        how : "absolute" | "relative" | "norm" | "no_anom"
            Method to calculate the anomaly. Default "absolute".
    """

    assert how in ("absolute", "relative", "norm", "no_anom", "no_check")

    if how == "no_check":
        return ds

    if ("year" in ds.dims) and ("time" in ds.dims):
        msg = "'year' and 'time' in dims"
        raise KeyError(msg)

    # if annual values have been calculated with groupby
    # (groupby('time.year').mean('year'))
    if "year" in ds.dims:
        years = ds.year
        time_str = "year"
    else:
        years = ds.time.dt.year
        start, end = str(start), str(end)
        time_str = "time"

    # check if time series spans reference period
    yr_min, yr_max = years.min(), years.max()
    if not time_in_range(int(start), int(end), yr_min, yr_max, metadata=metadata):
        return []

    selector = {time_str: slice(start, end)}

    if how != "no_anom":
        mean = ds.sel(**selector).mean(time_str, skipna=skipna)

    if how == "norm":
        std = ds.sel(**selector).std(time_str, skipna=skipna)

    if how == "no_anom":
        return ds
    elif how == "absolute":
        return ds - mean
    elif how == "relative":
        return (ds - mean) / mean * 100
    elif how == "norm":
        return (ds - mean) / std


def calc_year_of_warming_level(anomalies, warming_level):
    # calculate the start and end year of period of first exceedance

    anomalies = anomalies.rolling(year=20, center=True).mean()

    # find years warmer than 'warming_level'
    sel = anomalies - warming_level > 0.0

    # if no warmer year is found, return
    if not sel.any():
        return None, None, None

    # find index of central year
    idx = sel.argmax().values

    central_year = anomalies.isel(year=idx).year.values

    beg = int(central_year - 20 / 2)
    end = int(central_year + (20 / 2 - 1))

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
    add_meta=False,
    reduce="mean",
    select_by=("model", "exp", "ens"),
    factor=None,
):
    """ compute value of index at a several warming levels

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
        )

        if factor is not None:
            res *= factor

        out.append(res)

    return out


def at_warming_level(
    tas_list,
    index_list,
    warming_level,
    add_meta=False,
    reduce="mean",
    select_by=("model", "exp", "ens"),
):
    """ compute value of index at a certain warming level

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
    models = list()
    ensname = list()
    exp = list()

    # loop through all global mean temperatures
    for tas, metadata in tas_list:

        attributes = {key: metadata[key] for key in select_by}

        # try to find the index
        index = select_by_metadata(index_list, **attributes)

        # make sure only one dataset is found in index_list
        if len(index) != 1:
            raise ValueError(metadata)

        # an index was found for this tas dataset
        if index:

            # determine year when the warming was first reached
            beg, end, center = calc_year_of_warming_level(tas.tas, warming_level)

            # print(f"{beg} -- {end} {metadata['exp']} {metadata['model']} {metadata['ens']}")

            if beg:
                ds_idx = index[0][0]
                metadata_idx = index[0][1]

                # get the Dataarray
                da_idx = ds_idx[metadata_idx["varn"]]
                idx = da_idx.sel(year=slice(beg, end))

                if reduce is not None:
                    # calculate mean
                    idx = getattr(idx, reduce)("year")
                else:
                    # drop year to enable concatenating
                    idx = idx.drop_vars("year")

                models.append(metadata["model"])
                ensname.append(metadata["ens"])
                exp.append(metadata["exp"])

                out.append(idx)

    if not out:
        return []

    out = xr.concat(out, dim="ens", coords="minimal", compat="override")

    if add_meta:
        out = out.assign_coords(
            model=("ens", models), ensname=("ens", ensname), exp=("ens", exp)
        )
    return out


def match_data_list(list_a, list_b, select_by=("model", "exp", "ens"), check=True):

    out_a = list()
    out_b = list()

    # loop through all global mean temperatures
    for ds_a, metadata in list_a:

        attributes = {key: metadata[key] for key in select_by}

        # try to find the index
        match = select_by_metadata(list_b, **attributes)

        # make sure only one dataset is found in index_list
        if check and len(match) > 1:
            print(match)
            raise ValueError(metadata)

        # an index was found for this tas dataset
        if match:
            out_a += [[ds_a, metadata]]
            out_b += match

    return out_a, out_b


def concat_xarray_with_metadata(
    datalist,
    process=None,
    index={"mod_ens": ("model", "ens")},
    retain=("model", "ens", "ensnumber", "exp"),
):
    """create xr Dataset with 'ens' and 'model' as multiindex

        Input
        -----
        data : nested dict


    """

    all_ds = list()
    retain_dict = dict()

    retain_dict["ensi"] = ("ensi", list())
    for r in retain:
        retain_dict[r] = (r, list())

    for i, (ds, metadata) in enumerate(datalist):

        if process is not None:
            ds = process(ds)

        all_ds.append(ds)

        retain_dict["ensi"][1].append(i)
        for r in retain:
            retain_dict[r][1].append(metadata[r])

    # concate all data
    out = xr.concat(all_ds, "mod_ens")
    # assign coordinates
    out = out.assign_coords(**retain_dict)

    index = {"mod_ens": retain + ("ensi",)}

    # create multiindex
    out = out.set_index(**index)

    return out


def concat_xarray_without_metadata(datalist, process=None):
    """create xr Dataset with 'ens' and 'model' as multiindex

        Input
        -----
        data : nested dict


    """

    all_ds = list()

    for i, (ds, metadata) in enumerate(datalist):

        if process is not None:
            ds = process(ds)

        all_ds.append(ds)

    # concate all data
    out = xr.concat(all_ds, "ens", compat="override", coords="minimal")

    return out
