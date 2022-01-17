import numpy as np
import xclim
from xclim import atmos

from .. import xarray_utils as xru
from .transform_with_xarray import TransformWithXarray


class CDD(TransformWithXarray):
    def __init__(self, var="pr", freq="A", mask=None):
        """transformation function to calculate Consecutive Dry Days (CDD)

        Parameters
        ----------
        var : str, default: "pr"
            Name of the variable to extract.
        freq : str, default: "A"
            Resampling frequency.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        """

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


class TX_Days_Above(TransformWithXarray):
    def __init__(self, thresh="25.0 degC", var="tasmax", freq="A", mask=None):
        """
        transformation function to calculate Number of days where daily maximum
        temperature exceed a threshold

        Parameters
        ----------
        thresh : str, default: '25.0 degC'
            Threshold temperature on which to base evaluation [℃] or [K].
        var : str, default: "tasmax"
            Name of the variable to extract.
        freq : str, default: "A"
            Resampling frequency.
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        """

        self.thresh = thresh
        self.var = var
        self.freq = freq
        self.mask = mask

        self._name = "TX_Days_Above"

    def _trans(self, da, attrs):

        # rechunk into a single dask array chunk along time
        da = da.chunk({"time": -1})

        da = atmos.tx_days_above(da, thresh=self.thresh, freq=self.freq)

        # get rid of the "days" units, else CDD will have dtype = timedelta
        da.attrs.pop("units")

        return da, attrs


class VPD(TransformWithXarray):
    def __init__(self, var, rh, mask=None):
        """transformation function to calculate Vapour Pressure Deficit (VPD)

        Parameters
        ----------
        var : str, default: "tasmax"
            Name of the variable to extract.
        rh : xr.DataArray
            Relative humidity data
        mask : xr.DataArray, optional
            If given sets values in da to NaN where mask is False.
        """

        self.var = var
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
