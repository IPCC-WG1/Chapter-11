import numpy as np
import scipy as sp
import statsmodels.api as sm
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

        if all(
            a in attribs and (attribs[a] == attributes[a] or attributes[a] == "*")
            for a in attributes
        ):
            pass
        else:
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

    return xr.concat(out, dim="ens", coords="minimal", compat="override")


def mannwhitney(d1, d2):
    """Wilcoxon–Mann–Whitney-U test with Benjamini and Hochberg correction"""

    # make lat/ lon a 1D variable
    d1_stack = d1.stack(lat_lon=("lat", "lon"))
    d2_stack = d2.stack(lat_lon=("lat", "lon"))

    # create dummy array to store the results
    result = d1_stack.mean(("ens"))

    for i in range(result.lat_lon.shape[0]):

        # unpack ens/ time
        v1 = d1_stack.isel(lat_lon=i).values.ravel()
        v2 = d2_stack.isel(lat_lon=i).values.ravel()

        # only calculate if we actually have data
        if (~np.isnan(v1)).sum() > 0:
            _, p_val = sp.stats.mannwhitneyu(v1, v2)
        else:
            p_val = 1.0

        result[i] = p_val

    result = result.unstack("lat_lon")

    # apply Benjamini and Hochberg correction
    shape = result.shape
    p_adjust = sm.stats.multipletests(
        result.values.ravel(), alpha=0.05, method="fdr_bh"
    )[0]
    p_adjust = p_adjust.reshape(shape)

    result.values[:] = p_adjust

    return result


MANNWHITNEY_DICT = dict()


def get_mannwhitney(d1, d2, name):
    # cache the results of mannwhitney, as it is slow

    if name not in MANNWHITNEY_DICT.keys():

        mw = mannwhitney(d1, d2)

        MANNWHITNEY_DICT[name] = mw

    return MANNWHITNEY_DICT[name]


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


def concat_xarray_without_metadata(
    datalist,
    process=None,
):
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
    out = xr.concat(all_ds, "ens")

    return out
