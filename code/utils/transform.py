import xarray as xr
import os
from . import xarray_utils as xru

# prc.globmean(fNs_in, fN_out, metadata)(var, dim=('lon', 'lat'))  # noqa


# prc.process(fNs_in, fN_out, metadata).globmean(var, dim=('lon', 'lat'))  # noqa


# prc.globmean(fNs_in, fN_out, metadata).process(var, dim=('lon', 'lat'))  # noqa


# prc.globmean(var, dim=('lon', 'lat')).process(fNs_in, fN_out, metadata)  # noqa


# prc.regional_average(var, landmask=None, land_only=True).process(fNs_in, fN_out, metadata)  # noqa


# =============================================================================


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
    """docstring for globmean"""

    def __init__(self, var, dim=("lat", "lon")):

        self.var = var
        self.dim = dim
        self._name = "globmean"

    def _trans(self, ds):
        """transformation function to get a global average
        """

        wgt = xru.cos_wgt(ds)

        if len(ds) == 0:
            return []
        else:
            attrs = ds.attrs
            da = xru.average(ds[self.var], dim=self.dim, weights=wgt, keep_attrs=True)

            ds = da.to_dataset(name=self.var)

            ds.attrs = attrs

            return ds
