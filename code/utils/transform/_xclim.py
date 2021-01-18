import numpy as np
import xclim
from xclim import atmos

from .. import xarray_utils as xru
from .utils import _ProcessWithXarray


class CDD(_ProcessWithXarray):
    def __init__(self, var="pr", freq="A", mask=None):

        self.var = var
        self.freq = freq
        self.mask = mask

        self._name = "CDD"

    def _trans(self, da, attrs):

        # rechunk into a single dask array chunk along time
        da = da.chunk({"time": -1})

        da = atmos.maximum_consecutive_dry_days(da, freq=self.freq)

        # get rid of the "days" units, else CDD will have dtype = timedelta
        da.attrs.pop("units")

        return da, attrs


class TX_Days_Above(_ProcessWithXarray):
    def __init__(self, thresh="25.0 degC", var="tasmax", freq="A", mask=None):

        self.thresh = thresh
        self.var = var
        self.freq = freq
        self.mask = mask

        self._name = "CDD"

    def _trans(self, da, attrs):

        # rechunk into a single dask array chunk along time
        da = da.chunk({"time": -1})

        da = atmos.tx_days_above(da, thresh=self.thresh, freq=self.freq)

        # get rid of the "days" units, else CDD will have dtype = timedelta
        da.attrs.pop("units")

        return da, attrs


class VPD(_ProcessWithXarray):
    def __init__(self, rh, mask=None):
        """calculate Vapour Pressure Deficit (VPD)

        Parameters
        ----------
        rh : xr.DataArray
            Relative humidity data
        mask : xr.DataArray, default None
            Mask to mask out data

        """

        self.rh = rh
        self.mask = mask

        self._name = "VPD"

    def _trans(self, da, attrs):

        rh = self.rh

        # allow values > 100 because there are...
        rh = xru.check_range(rh, min_allowed=0.0, max_larger=50)
        # constrain to 100: VPD shoould not be smaller than 0
        rh = np.fmin(rh, 100)

        rh = xru.maybe_reindex(rh, da)

        if rh is None:
            return [], attrs

        vp_sat = xclim.indices.saturation_vapor_pressure(da)

        vpd = vp_sat * (1 - rh / 100)

        return vpd, attrs
