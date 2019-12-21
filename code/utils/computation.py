import numpy as np
import xarray as xr


def time_in_range(start, end, yr_min, yr_max, metadata):
    """determine if start--end is in time vector"""

    #     print(start, yr_min, end, yr_max)
    if (start < yr_min) or (end > yr_max):
        msg = f"no data for {start} - {end} ({yr_min.values}..{yr_max.values})"

        if metadata is not None:
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
    idx = np.argmax(sel > 0).values

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
    for data, attribs in datalist:

        if all(
            a in attribs and (attribs[a] == attributes[a] or attributes[a] == "*")
            for a in attributes
        ):
            selection.append((data, attribs))
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
    for data, attribs in datalist:

        if any(
            a in attribs and (attribs[a] != attributes[a])
            for a in attributes
        ):
            selection.append((data, attribs))
    return selection


def at_warming_level(tas_list, index_list, warming_level):
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
    """

    out = list()

    # loop through all global mean temperatures
    for tas, metadata in tas_list:

        # try to find the index
        index = select_by_metadata(
            index_list,
            model=metadata["model"],
            exp=metadata["exp"],
            ens=metadata["ens"],
        )

        # make sure only one dataset is found in index_list
        assert len(index) <= 1, metadata

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
                idx = da_idx.sel(year=slice(beg, end)).mean("year")
                out.append(idx)

    return xr.concat(out, dim="ens")
