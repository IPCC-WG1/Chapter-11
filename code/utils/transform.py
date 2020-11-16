import numpy as np
import regionmask
import xarray as xr
from xclim import atmos

from . import xarray_utils as xru


class _ProcessWithXarray:

    _name = None

    def __call__(self, ds):

        # handle non-existing data
        if len(ds) == 0:
            return []
        else:
            # get attrs
            attrs = ds.attrs

            # read single variable
            da = ds[self.var]

            # apply the transformation funcion
            da, attrs = self._trans(da, attrs)

            # back to dataset again
            ds = da.to_dataset(name=self.var)

            # add the attrs again
            ds.attrs = attrs

        return ds

    def _trans(self, da, attrs):
        raise NotImplementedError("Implement _trans in the subclass")

    @property
    def name(self):
        if self._name is None:
            raise NotImplementedError("Please define a name")
        return self._name


def _get_func(object, how):
    """get a function by name"""

    func = getattr(object, how, None)

    if func is None:
        raise KeyError(f"how cannot be '{how}'")

    return func


class NoTransform(_ProcessWithXarray):
    """transformation which does nothing"""

    def __init__(self, var):

        self.var = var
        self._name = "no_transform"

    def _trans(self, da, attrs):

        return da, attrs


class Globmean(_ProcessWithXarray):
    """transformation function to get a global average"""

    def __init__(self, var, dim=("lat", "lon")):

        self.var = var
        self.dim = dim
        self._name = "globmean"

    def _trans(self, da, attrs):

        wgt = xru.cos_wgt(da)
        da = da.weighted(wgt).mean(dim=self.dim, keep_attrs=True)

        return da, attrs


class CDD(_ProcessWithXarray):
    def __init__(self, var="pr", freq="A"):

        self.var = var
        self.freq = freq
        self._name = "CDD"

    def _trans(self, da, attrs):

        # rechunk into a single dask array chunk along time
        da = da.chunk({"time": -1})

        da = atmos.maximum_consecutive_dry_days(da, freq=self.freq)

        # get rid of the "days" units, else CDD will have dtype = timedelta
        da.attrs.pop("units")

        return da, attrs


class TX_Days_Above(_ProcessWithXarray):
    def __init__(self, thresh="25.0 degC", var="tasmax", freq="A"):

        self.thresh = thresh
        self.var = var
        self.freq = freq
        self._name = "CDD"

    def _trans(self, da, attrs):

        # rechunk into a single dask array chunk along time
        da = da.chunk({"time": -1})

        da = atmos.tx_days_above(da, thresh=self.thresh, freq=self.freq)

        # get rid of the "days" units, else CDD will have dtype = timedelta
        da.attrs.pop("units")

        return da, attrs


class Resample(_ProcessWithXarray):
    """transformation function to resample by year"""

    def __init__(self, indexer, var, how, **kwargs):

        self.indexer = indexer
        self.var = var
        self.how = how
        self._name = "resample_annual_" + how
        self.kwargs = kwargs

    def _trans(self, da, attrs):

        resampler = da.resample(self.indexer)

        func = _get_func(resampler, self.how)

        if self.how == "quantile":
            da = da.load()

        da = func(dim="time", **self.kwargs)

        return da, attrs


class ResampleAnnual(Resample):
    """transformation function to resample by year"""

    def __init__(self, var, how, **kwargs):

        self.indexer = {"time": "A"}
        self.var = var
        self.how = how
        self._name = "resample_annual_" + how
        self.kwargs = kwargs


#     def _trans(self, da, attrs):

#         resampler = da.resample(time="A")

#         func = _get_func(resampler, self.how)

#         if self.how == "quantile":
#             da = da.load()

#         da = func(dim="time", **self.kwargs)

#         return da, attrs


class ResampleMonthly(Resample):
    """transformation function to resample by month"""

    def __init__(self, var, how, **kwargs):

        self.indexer = {"time": "M"}
        self.var = var
        self.how = how
        self._name = "resample_monthly_" + how
        self.kwargs = kwargs


#     def _trans(self, da, attrs):

#         resampler = da.resample(time="M")
#         func = _get_func(resampler, self.how)

#         if self.how == "quantile":
#             da = da.load()

#         da = func(dim="time", **self.kwargs)

#         return da, attrs


class ResampleSeasonal(Resample):
    """transformation function to resample by month"""

    def __init__(self, var, how, invalidate_beg_end, **kwargs):

        self.indexer = {"time": "Q-FEB"}
        self.var = var
        self.how = how
        self._name = "resample_seasonal_" + how
        self.invalidate_beg_end = invalidate_beg_end
        self.kwargs = kwargs

    def _trans(self, da, attrs):

        # call the parent method
        da, attrs = super()._trans(da, attrs)

        # as DJF is not complete we can set it to NaN here
        if self.invalidate_beg_end:
            da[{"time": [0, -1]}] = np.NaN

        return da, attrs


#         resampler = da.resample(time="M")
#         func = _get_func(resampler, self.how)

#         if self.how == "quantile":
#             da = da.load()

#         da = func(dim="time", **self.kwargs)

#         return da, attrs


class RollingResampleAnnual(_ProcessWithXarray):
    """transformation function to resample by year"""

    def __init__(self, var, window, how_rolling, how, skipna=False, **kwargs):

        self.var = var
        self.window = window
        self.how_rolling = how_rolling
        self.how = how
        self._name = f"rolling_{how_rolling}_{window}_resample_annual_{how}"
        self.kwargs = kwargs

    def _trans(self, da, attrs):

        rolling = da.rolling(time=self.window)
        func = _get_func(rolling, self.how_rolling)
        # its much less memory intensive with skipna=False
        da = func(skipna=self.skipna)

        resampler = da.resample(time="A")
        func = _get_func(resampler, self.how)

        if self.how == "quantile":
            da = da.load()

        da = func(dim="time", **self.kwargs)

        return da, attrs


class GroupbyAnnual(_ProcessWithXarray):
    """transformation function to GroupBy year"""

    def __init__(self, var, how):

        self.var = var
        self.how = how
        self._name = "groupby_annual_" + how

    def _trans(self, da, attrs):

        grouper = da.groupby("time.year")
        func = _get_func(grouper, self.how)

        da = func("time")

        return da, attrs


class SelectGridpoint(_ProcessWithXarray):
    """transformation function to select a gridpoint"""

    def __init__(self, var, **coords):

        self.var = var
        self.coords = coords
        name = "__".join([f"{key}_{value}" for key, value in coords.items()])
        self._name = "sel_" + name

    def _trans(self, da, attrs):
        # TODO: normalize to 0..360 or -180..180?

        da = da.sel(**self.coords, method="nearest")

        return da, attrs


class SelectRegion(_ProcessWithXarray):
    """transformation function to subset a square region"""

    def __init__(self, var, **coords):

        self.var = var
        self.coords = coords
        name = "__".join(
            [f"{key}_{value.start}_{value.stop}" for key, value in coords.items()]
        )
        self._name = "sel_" + name

    def _trans(self, da, attrs):

        # TODO: normalize to 0..360 or -180..180?

        da = da.sel(**self.coords)

        return da, attrs


class RegionAverage(_ProcessWithXarray):
    """calculate regional average"""

    def __init__(self, var, regions, landmask=None, land_only=True, weights=None):
        """
        Parameters
        ----------
        var : string
            Name of the variable to treat
        regions : regionmask.Regions
           regions to take the average over.
        landmask : DataArray, optional
            landmaks to use, land points must be ``True``. If none uses
            regionmask.defined_regions.natural_earth.land_110.
       land_only : bool, optional
           Whether to mask out ocean points before calculating regional
           means.
    """

        self.var = var
        self.regions = regions
        self.landmask = landmask
        self.land_only = land_only
        self.weights = weights

        if not isinstance(regions, regionmask.Regions):
            raise ValueError("'regions' must be a regionmask.Regions instance")

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

        from . import regions

        if self.weights is None:
            # get cosine weights
            weight = xru.cos_wgt(da)

        if self.landmask is None:
            landmask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            landmask = landmask.squeeze(drop=True)

        if landmask.max() > 1.0 or landmask.min() < 0.0:
            msg = "landmask must be in the range 0..1. Found values {}..{}"
            msg = msg.format(landmask.min().values, landmask.max().values)
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

        da = da.weighted(mask_3D * weight).mean(("lat", "lon"))

        return da, attrs
