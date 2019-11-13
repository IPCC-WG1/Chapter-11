import numpy as np

from conf import ANOMALY_YR_START, ANOMALY_YR_END


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


def calc_anomaly(
    ds,
    start=ANOMALY_YR_START,
    end=ANOMALY_YR_END,
    how="absolute",
    skipna=None,
    metadata=None,
):
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

    assert how in ("absolute", "relative", "norm", "no_anom")

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
