from xclim import atmos

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
