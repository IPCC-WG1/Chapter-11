from .utils import _ProcessWithXarray, alignable


class SM_dry_days_clim(_ProcessWithXarray):
    def __init__(self, var, quantile=0.1, clim=slice("1850", "1900"), dim="time"):
        """calc climatology of SM dry days

        Parameters
        ----------
        var : str
            Name of the variable on the Dataset
        quantile : float
            Quantile in range 0..1, default: 0.1
        clim : slice(str, str)
            Climatology period, default: slice("1850", "1900")
        """

        self.var = var
        self.quantile = quantile
        self.clim = clim
        self.dim = dim
        self._name = f"SM_dry_days_clim_c{clim.start}-{clim.stop}_q{quantile}"

    def _trans(self, da, attrs):

        da = da.sel(**{self.dim: self.clim})

        if len(da[self.dim]) == 0:
            return [], attrs

        # required for quantile
        da = da.chunk({self.dim: -1})
        da = da.groupby(f"{self.dim}.month")
        da = da.quantile(self.quantile, dim=self.dim, skipna=False)

        return da, attrs


class SM_dry_days(_ProcessWithXarray):
    def __init__(self, var, dim="time"):
        """calc climatology of SM dry days

        Parameters
        ----------
        var : str
            Name of the variable on the Dataset
        quantile : float
            Quantile in range 0..1, default: 0.1
        clim : slice(str, str)
            Climatology period, default: slice("1850", "1900")
        """

        self.var = var
        self.dim = dim
        self._name = "SM_dry_days"

    def _trans(self, da, attrs, threshold):

        if not alignable(da, threshold):
            raise ValueError("not alignable")

        da = da.groupby(f"{self.dim}.month")

        # select the variable from threshold, else we end up with a Dataset,
        # as threshold is from a climatology of the same data can use self.var
        threshold = threshold[self.var]

        # find days where SM is below the threshold
        da = da < threshold[self.var]

        # count the number of days per year
        da = da.resample(**{self.dim: "A"}).sum()

        return da, attrs
