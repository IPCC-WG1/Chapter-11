import numpy as np
import xarray as xr
from xclim import atmos

import regionmask

from . import xarray_utils as xru


class _ProcessWithXarray:

    _name = None

    def __call__(self, ds):
        return self._trans(ds)

    def _trans(self, ds):
        raise NotImplementedError("Implement _trans in the subclass")

    @property
    def name(self):
        if self._name is None:
            raise NotImplementedError("Please define a name")
        return self._name


class NoTransform(_ProcessWithXarray):
    """transformation which does nothing"""

    def __init__(self, var):

        self.var = var
        self._name = "no_transform"

    def _trans(self, ds):

        if len(ds) == 0:
            return []
        else:

            # check variable is available
            da = ds[self.var]

            return ds

class Globmean(_ProcessWithXarray):
    """transformation function to get a global average"""

    def __init__(self, var, dim=("lat", "lon")):

        self.var = var
        self.dim = dim
        self._name = "globmean"

    def _trans(self, ds):

        if len(ds) == 0:
            return []
        else:
            wgt = xru.cos_wgt(ds)

            attrs = ds.attrs

            da = ds[self.var]
            da = xru.average(da, dim=self.dim, weights=wgt, keep_attrs=True)
            ds = da.to_dataset(name=self.var)

            ds.attrs = attrs

            return ds


class CDD(_ProcessWithXarray):

    def __init__(self, var="pr", freq='A'):

        self.var = var
        self.freq = freq
        self._name = "CDD"

    def _trans(self, ds):

        if len(ds) == 0:
            return []
        else:

            attrs = ds.attrs
            da = ds[self.var]

            # rechunk into a single dask array chunk along time
            da = da.chunk({'time': -1})

            da = atmos.maximum_consecutive_dry_days(da, freq=self.freq)

            # get rid of the "days" units, else CDD will have dtype = timedelta
            da.attrs.pop("units")

            ds = da.to_dataset(name=self.var)
            ds.attrs = attrs

            return ds

class TX_Days_Above(_ProcessWithXarray):

    def __init__(self, thresh="25.0 degC", var="tasmax", freq='A'):

        self.thresh = thresh
        self.var = var
        self.freq = freq
        self._name = "CDD"

    def _trans(self, ds):

        if len(ds) == 0:
            return []
        else:

            attrs = ds.attrs
            da = ds[self.var]

            # rechunk into a single dask array chunk along time
            da = da.chunk({'time': -1})

            da = atmos.tx_days_above(da, thresh=self.thresh, freq=self.freq)

            # get rid of the "days" units, else CDD will have dtype = timedelta
            da.attrs.pop("units")

            ds = da.to_dataset(name=self.var)
            ds.attrs = attrs

            return ds

class ResampleAnnual(_ProcessWithXarray):
    """transformation function to resample by year"""

    def __init__(self, var, how, **kwargs):

        self.var = var
        self.how = how
        self._name = "resample_annual_" + how
        self.kwargs = kwargs

    def _trans(self, ds):

        if len(ds) == 0:
            return []
        else:
            attrs = ds.attrs

            da = ds[self.var]
            resampler = da.resample(time="A")

            func = getattr(resampler, self.how, None)

            if func is None:
                raise KeyError(f"how cannot be '{self.how}'")

            if self.how == "quantile":
                ds = ds.load()

            da = func(dim="time", **self.kwargs)

            ds = da.to_dataset(name=self.var)
            ds.attrs = attrs

        return ds


class GroupbyAnnual(_ProcessWithXarray):
    """transformation function to GroupBy year"""

    def __init__(self, var, how):

        self.var = var
        self.how = how
        self._name = "groupby_annual_" + how

    def _trans(self, ds):

        if len(ds) == 0:
            return []
        else:
            attrs = ds.attrs

            da = ds[self.var]
            grouper = da.groupby("time.year")

            func = getattr(grouper, self.how, None)

            if func is None:
                raise KeyError(f"how cannot be '{self.how}'")

            da = func("time")

            ds = da.to_dataset(name=self.var)
            ds.attrs = attrs

        return ds


class RegionAverage(_ProcessWithXarray):
    """calculate regional average"""


    def __init__(self, var, regions, landmask=None, land_only=True):
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

        if not isinstance(regions, regionmask.Regions):
            raise ValueError("'regions' must be a regionmask.Regions instance")

        self._name = "region_average_" + regions.name

        abbrevs = ["global", "ocean", "land", "land_wo_antarctica"]
        self.abbrevs = abbrevs + regions.abbrevs

    def _trans(self, ds):
        """
        Parameters
        ----------
        da : DataArray
            Object over which the weighted reduction operation is applied.

        """
        attrs = ds.attrs

        da = ds[self.var]

        # get cosine weights
        weight = xru.cos_wgt(da)

        numbers = np.array(self.regions.numbers)

        if self.landmask is None:
            landmask = regionmask.defined_regions.natural_earth.land_110.mask(da)
            landmask = landmask == 0

        if self.land_only:
            wgt = weight * landmask
        else:
            # we need to add lon coordinates
            wgt = xr.full_like(landmask, 1) * weight

        mask = self.regions.mask(da)

        # list to accumulate averages
        ave = list()

        # global mean
        a = xru.average(da, dim=("lat", "lon"), weights=weight)
        ave.append(a)

        # global ocean mean
        a = xru.average(da, dim=("lat", "lon"), weights=(weight * (1.0 - landmask)))
        ave.append(a)

        # global land mean
        a = xru.average(da, dim=("lat", "lon"), weights=(weight * landmask))
        ave.append(a)

        # global land mean w/o antarctica
        da_selected = da.sel(lat=slice(-60, None))
        a = xru.average(da_selected, dim=("lat", "lon"), weights=(weight * landmask))
        ave.append(a)

        # it is faster to calculate the weighted mean via groupby
        g = da.groupby(mask)
        wgt_stacked = wgt.stack(stacked_lat_lon=("lat", "lon"))
        a = g.apply(xru.average, dim=("stacked_lat_lon"), weights=wgt_stacked)
        ave.append(a.drop("region"))

        da = xr.concat(ave, dim="region")

        # shift region coordinates such that 1 to 26 corresponds to the
        # regions

        numbers = np.arange(numbers.min() - 4, numbers.max() + 1)
        #         da.region.values[:] = x

        # add the abbreviations of the regions, update the numbers
        da = da.assign_coords(
            **{"abbrev": ("region", self.abbrevs), "region": ("region", numbers)}
        )

        ds = da.to_dataset(name=self.var)

        ds.attrs = attrs
        return ds
