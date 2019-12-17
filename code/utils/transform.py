import xarray as xr
import numpy as np

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

            da = func("time", **self.kwargs)

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
    """transformation function to GroupBy year"""

    def __init__(self, var, regions, mask=None, land_only=True):

        self.var = var
        self.regions = regions
        self.mask = mask
        self.land_only = land_only

        if not isinstance(regions, regionmask.Regions):
            raise ValueError("'regions' must be a regionmask.Regions instance")

        self._name = "region_average_" + regions.name

        abbrevs = ["global", "ocean", "land", "land_wo_antarctica"]
        self.abbrevs = abbrevs + regions.abbrevs

    def _trans(self, ds):

        attrs = ds.attrs

        da = ds[self.var]

        # get cosine weights
        weight = xru.cos_wgt(da)

        numbers = np.array(self.regions.numbers)

        if self.mask is None:
            mask = regionmask.defined_regions.natural_earth.land_110.mask(da)
            mask = mask == 0

        if self.land_only:
            wgt = weight * mask
        else:
            wgt = weight

        mask = self.regions.mask(da)

        # list to accumulate averages
        ave = list()

        # global mean
        a = xru.average(da, dim=("lat", "lon"), weights=weight)
        ave.append(a)

        # global ocean mean
        a = xru.average(da, dim=("lat", "lon"), weights=(weight * (1.0 - mask)))
        ave.append(a)

        # global land mean
        a = xru.average(da, dim=("lat", "lon"), weights=(weight * mask))
        ave.append(a)

        # global land mean w/o antarctica
        da_selected = da.sel(lat=slice(-60, None))
        a = xru.average(da_selected, dim=("lat", "lon"), weights=wgt)
        ave.append(a)

        # it is faster to calculate the weighted mean via groupby
        g = da.groupby(mask)
        wgt_stacked = wgt.stack(stacked_lat_lon=("lat", "lon"))
        a = g.apply(xru.average, dim=("stacked_lat_lon"), weights=wgt_stacked)
        ave.append(a.drop("region"))

        da = xr.concat(ave, dim="region")

        # shift srex coordinates such that 1 to 26 corresponds to the
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
