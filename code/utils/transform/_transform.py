import numpy as np
import regionmask
import xarray as xr

from .. import xarray_utils as xru
from .transform_with_xarray import TransformWithXarray


def _get_func(obj, how):
    """get a function by name"""

    func = getattr(obj, how, None)

    if func is None:
        raise KeyError(f"how cannot be '{how}'")

    return func


class NoTransform(TransformWithXarray):
    def __init__(self, var, mask=None):
        """transformation which does nothing (except maybe masking)

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        """

        self.var = var
        self.mask = mask
        self._name = "no_transform"

    def _trans(self, da, attrs):

        return da, attrs


class Globmean(TransformWithXarray):
    def __init__(self, var, weights=None, mask=None, dim=("lat", "lon")):
        """transformation function to calculate the area-weighted global average

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        weights : xr.DataArray
            Used as area weights for the weighted mean. Uses the cosine of lat if
            None is passed.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        dim : str, iterable of str, default: ("lat", "lon")
            Dimensions to average over.
        """

        # TODO: make weights mandatory?

        self.var = var
        self.weights = weights
        self.mask = mask
        self.dim = dim

        self._name = "globmean"

    def _trans(self, da, attrs):

        # maybe get cosine weights
        weights = xru.cos_wgt(da) if self.weights is None else self.weights

        da = da.weighted(weights).mean(dim=self.dim, keep_attrs=True)

        return da, attrs


class _Resample(TransformWithXarray):
    def __init__(self, indexer, var, how, mask=None, **kwargs):
        """base transformation function to resample by any frequency

        Parameters
        ----------
        indexer : dict
            Mapping from the dimension name to resample frequency. The dimension must be
            datetime-like.
        var : str
            Name of the variable to extract.
        how : str
            Which reduction to apply, e.g. "mean", "std".
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        kwargs : Additional arguments passed to the reduction method (e.g. ``skipna``).
        """

        self.indexer = indexer
        self.var = var
        self.how = how
        self.mask = mask
        self.kwargs = kwargs

        self._name = "resample_" + how

    def _trans(self, da, attrs):

        resampler = da.resample(self.indexer)

        func = _get_func(resampler, self.how)

        if self.how == "quantile":
            da = da.load()

        da = func(dim="time", **self.kwargs)

        return da, attrs


class ResampleAnnual(_Resample):
    def __init__(self, var, how, mask=None, **kwargs):
        """transformation function to resample by year

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        how : str
            Which reduction to apply, e.g. "mean", "std".
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        kwargs : Additional arguments passed to the reduction method (e.g. ``skipna``).
        """

        self.indexer = {"time": "A"}
        self.var = var
        self.how = how
        self.mask = mask
        self.kwargs = kwargs

        self._name = "resample_annual_" + how


class ResampleMonthly(_Resample):
    def __init__(self, var, how, mask=None, **kwargs):
        """transformation function to resample by month

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        how : str
            Which reduction to apply, e.g. "mean", "std".
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        kwargs : Additional arguments passed to the reduction method (e.g. ``skipna``).
        """

        self.indexer = {"time": "M"}
        self.var = var
        self.how = how
        self.mask = mask
        self.kwargs = kwargs

        self._name = "resample_monthly_" + how


class ResampleSeasonal(_Resample):
    def __init__(self, var, how, invalidate_beg_end, mask=None, **kwargs):
        """transformation function to resample by season

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        how : str
            Which reduction to apply, e.g. "mean", "std".
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        invalidate_beg_end : bool
            If True sets the first and last timestep to NaN (because DJF is not
            complete). If False the two timesteps are kept as is.
        kwargs : Additional arguments passed to the reduction method (e.g. ``skipna``).
        """

        self.indexer = {"time": "Q-FEB"}
        self.var = var
        self.how = how
        self.invalidate_beg_end = invalidate_beg_end
        self.mask = mask
        self.kwargs = kwargs

        self._name = "resample_seasonal_" + how

    def _trans(self, da, attrs):

        # call the parent method
        da, attrs = super()._trans(da, attrs)

        # as DJF is not complete we can set it to NaN here
        if self.invalidate_beg_end:
            da[{"time": [0, -1]}] = np.NaN

        return da, attrs


class RollingResampleAnnual(_Resample):
    def __init__(
        self, var, window, how_rolling, how, skipna=False, mask=None, **kwargs
    ):
        """
        transformation function to first apply a rolling operation and then resample by year

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        window : int
            Moving window size.
        how_rolling : str
            Which reduction to apply to the rolling function, e.g. "mean", "std".
        how : str
            Which reduction to apply for the annual resample, e.g. "mean", "std".
        skipna : bool, default, False
            If True, skip missing values (as marked by NaN) of the rolling operation.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        kwargs : Additional arguments passed to the reduction method of the annual resample
            operation (e.g. ``skipna``).

        """

        self.indexer = {"time": "A"}
        self.var = var
        self.window = window
        self.how_rolling = how_rolling
        self.how = how
        self.skipna = skipna
        self.mask = mask
        self.kwargs = kwargs

        self._name = f"rolling_{how_rolling}_{window}_resample_annual_{how}"

    def _trans(self, da, attrs):

        # 1. apply a rolling operation
        rolling = da.rolling(time=self.window)
        func = _get_func(rolling, self.how_rolling)
        # its much less memory intensive with skipna=False
        da = func(skipna=self.skipna)

        # 2. resample to annual (call the parent method)
        da, attrs = super()._trans(da, attrs)

        return da, attrs


class SelectGridpoint(TransformWithXarray):
    def __init__(self, var, mask=None, **coords):
        """transformation function to select a gridpoint

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        coords : dict
            Keyword arguments of keys matching dimensions and values given by scalars.
        """

        self.var = var
        self.coords = coords
        self.mask = mask

        name = "__".join([f"{key}_{value}" for key, value in coords.items()])
        self._name = "sel_" + name

    def _trans(self, da, attrs):
        # TODO: normalize to 0..360 or -180..180?

        da = da.sel(**self.coords, method="nearest")

        return da, attrs


class SelectRegion(TransformWithXarray):
    def __init__(self, var, mask=None, **coords):
        """transformation function to subset a square region

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        coords : dict
            Keyword arguments of keys matching dimensions and values given by slices.
        """

        self.var = var
        self.coords = coords
        self.mask = mask

        name = "__".join(
            f"{key}_{value.start}_{value.stop}" for key, value in coords.items()
        )
        self._name = f"sel_{name}"

    def _trans(self, da, attrs):

        # TODO: normalize to 0..360 or -180..180?

        da = da.sel(**self.coords)

        return da, attrs


class ConsecutiveMonthsClim(TransformWithXarray):
    def __init__(self, var, how, *, clim=slice("1850", "1900"), dim="time", mask=None):
        """
        transformation function to calculate climatological min/ max of consecutive
        months (i.e. rolling with wrap-around)

        Parameters
        ----------
        var : str
            Name of the variable to extract.
        how : "min" | "max"
            Which reduction to apply, e.g. "mean", "std".
        clim : slice(str, str), default: slice("1850", "1900")
            Climatology period
        dim : str
            Name of the time dimension.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        """

        if how not in ("min", "max"):
            raise ValueError(f"how must be one of 'min', 'max', found {how}")

        self.var = var
        self.how = how
        # TODO: make this an option - need to adjust the loop below
        self.n_months = 3
        self.clim = clim
        self.dim = dim
        self._name = f"ConsecutiveMonths_{clim.start}-{clim.stop}_{how}_{self.n_months}"
        self.mask = mask

    def _trans(self, da, attrs):

        da = da.sel(**{self.dim: self.clim})

        if len(da[self.dim]) == 0:
            return [], attrs

        n_months = self.n_months

        # calculate the monthly climatology
        monthly = da.groupby(f"{self.dim}.month").mean(skipna=False)

        # TODO: use padded rolling once available
        # monthly.rolling(center=True, month=n_months, pad_mode="wrap").mean(skipna=False)

        # pad
        padded = monthly.pad(month=n_months, mode="wrap")

        # calculate the rolling mean
        rolled = padded.rolling(center=True, month=n_months).mean(skipna=False)

        # remove the padding again
        sliced = rolled.isel(month=slice(n_months, -n_months))

        # find coordinates (e.g. idxmax)
        central_month = getattr(sliced, f"idx{self.how}")("month")

        all_nan = central_month.isnull()
        # the index
        central_month_arg = (central_month.fillna(0) - 1).astype(int)

        # create a mask
        month_mask = xr.zeros_like(monthly, bool)

        # set true
        for i in range(-1, 2):
            # the "% 12" normalizes the index 12 -> 0; 13 -> 1; -1 -> 11
            idx = (central_month_arg + i) % 12
            month_mask[{"month": idx}] = True

        # remove all nan gridpoints
        month_mask = month_mask.where(~all_nan, False)

        central_month.name = "central_month"
        month_mask.name = "month_mask"

        ds = xr.merge([central_month, month_mask])

        return ds, attrs


class RegionAverage(TransformWithXarray):
    def __init__(self, var, regions, landmask=None, land_only=True, weights=None):
        """transformation function to calculate regional average

        Parameters
        ----------
        var : string
            Name of the variable to extract.
        regions : regionmask.Regions
            Regions to take the average over.
        landmask : DataArray, optional
            landmask or landfraction to use, land points must be ``1``. If None uses
            regionmask.defined_regions.natural_earth.land_110.
        land_only : bool, default: True
            Whether to mask out ocean points before calculating regional means.
        weights : xr.DataArray
            Used as area weights for the weighted mean. Uses the cosine of lat if None
            is passed.
        """

        self.var = var
        self.regions = regions
        self.landmask = landmask
        self.land_only = land_only
        self.weights = weights

        # hard-code mask to None -> use landmask instead
        self.mask = None

        if not isinstance(regions, regionmask.Regions):
            raise ValueError("``regions`` must be a ``regionmask.Regions`` instance")

        if regions.name is None:
            raise ValueError("regions require a name")

        self._name = "region_average_" + regions.name.replace(" ", "_")

    def _trans(self, da, attrs):
        """
        Parameters
        ----------
        da : DataArray
            Object over which the weighted reduction operation is applied.
        """

        weights = self._get_weights(da)
        mask_3D = self._get_mask3D(da)

        da = da.weighted(mask_3D * weights).mean(("lat", "lon"))

        return da, attrs

    def _get_weights(self, da):
        # maybe get cosine weights
        weights = xru.cos_wgt(da) if self.weights is None else self.weights
        xru.assert_alignable(weights, da, message="weights have different coordinates!")

        return weights

    def _get_mask3D(self, da):

        from .. import regions

        if self.landmask is None:
            landmask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            landmask = landmask.squeeze(drop=True)
        else:
            landmask = self.landmask
            xru.assert_alignable(
                landmask, da, message="landmask has different coordinates!"
            )

        if landmask.max() > 1.0 or landmask.min() < 0.0:
            mn, mx = landmask.min().item(), landmask.max().item()
            msg = f"landmask must be in the range 0..1. Found values {mn}..{mx}"
            raise ValueError(msg)

        # prepend the global regions
        numbers = np.array(self.regions.numbers)
        numbers_global = np.arange(numbers.min() - 4, numbers.min())
        global_mask_3D = regions.global_mask_3D(
            da, landmask=landmask, numbers=numbers_global
        )

        regional_mask_3D = self.regions.mask_3D(da)

        if self.land_only:
            regional_mask_3D = regional_mask_3D * landmask

        mask_3D = xr.concat([global_mask_3D, regional_mask_3D], dim="region")

        return mask_3D
