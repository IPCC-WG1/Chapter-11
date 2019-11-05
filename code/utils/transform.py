import xarray as xr
import os
from . import xarray_utils as xru


class _ProcessWithXarray:

    _name = None

    def __call__(self, ds):
        return self._trans(ds)

    def _trans(ds):
        raise NotImplementedError

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
    """transformation function to get a global average"""

    def __init__(self, var, how):

        self.var = var
        self._name = "resample_annual_" + how

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

            da = func("time")

            ds = da.to_dataset(name=self.var)
            ds.attrs = attrs

        return ds
