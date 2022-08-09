import warnings

import numpy as np
import xarray as xr

from .transform_with_xarray import TransformWithXarray


class IAV(TransformWithXarray):
    def __init__(
        self,
        var,
        period=20,
        min_length=500,
        cut_start=100,
        deg=2,
        dim="time",
        mask=None,
    ):
        """calc internal variability - AR5 method

        Parameters
        ----------
        var : str
            Name of the variable on the Dataset
        period : int
            Number of years to average over before calculating the IAV, default: 20
        min_length : int
            Minimum length of the dataset to be considered, default: 500
        cut_start : int
            Numbers of years that are removed at the beginning of the data,
            default: 100
        deg : int or None
            Degree of the polynom used to detrend. If None no detrending is applied.
            Default: 2.
        dim : str, default: "time"
            Dimension along which to apply the transformation.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        """

        self.var = var
        self.period = period
        self.min_length = min_length
        self.cut_start = cut_start
        self.deg = deg
        self.dim = dim
        self.mask = mask

        self._name = f"IAV_p{period}_m{min_length}_c{cut_start}_d{deg}"

    def _trans(self, da, attrs):

        # check length of da and cut
        da = _sanitize(da, attrs, self.min_length, self.cut_start)

        if da is None:
            return [], attrs

        da = _calc_n_year_means(da, self.period)

        da = _detrend(da, self.deg)

        da = da.std("time")
        return da, attrs


def _sanitize(da, attrs, min_length, cut_start):

    years = da.time.dt.year

    # does currently not handle non-unique years
    if len(years) != len(np.unique(years)):
        raise ValueError(f"model has non-unique years\n {attrs}")

    # check if there are years with all NaN data
    invalid_data = da.isnull().all(("lat", "lon"))

    if invalid_data.any():
        n = invalid_data.sum().item()
        warnings.warn(f"Found {n} all-nan years \n{attrs}", RuntimeWarning)

    # drop invalid years
    da = da.sel(time=~invalid_data)

    # skip piControl simulations that are less than 500 years
    if len(da.time) < min_length:
        return None

    # cut the first n years to minimize drift
    da = da.isel(time=slice(cut_start, None))

    return da


def _calc_n_year_means(da, period):

    # no need if period == 1
    if period > 1:

        # number of full periods in da
        n_periods = len(da.time) // period

        # cut years at the end so that len(da.time) is divisible by 'period'
        da = da.isel(time=slice(None, n_periods * period))

        # create groups with 'period' elements each
        groups = np.arange(len(da.time)) // period
        idx = xr.DataArray(groups, dims=["time"])

        # calculate 'period'-year means
        da = da.groupby(idx).mean()
        da = da.rename(group="time")
    else:
        # assign the year as time coordinate

        year = da.time.dt.year
        da = da.assign_coords(time=("time", year))

    return da


def _detrend(da, deg):

    if deg is not None:
        # normalize the time: this is stabler for polyfit

        time = da.time - da.time.mean()
        da = da.assign_coords(time=time)

        # detrend the with a polynom of order 'deg'
        polif = da.polyfit("time", deg=deg)
        da = da - xr.polyval(da.time, polif.polyfit_coefficients)

    return da
