import numpy as np
import xarray as xr

from .. import xarray_utils as xru
from .utils import _ProcessWithXarray


class SM_dry_days_clim_Zhang(_ProcessWithXarray):
    def __init__(self, var, quantile=0.1, beg=1850, end=1900, dim="time"):
        """calc climatology of SM dry days after Zhang 2005

        Parameters
        ----------
        var : str
            Name of the variable on the Dataset
        quantile : float
            Quantile in range 0..1, default: 0.1
        beg : int, default: 1850
            Start of climatology period.
        end : int, default: 1900
            End of climatology period.

        References
        ----------
        https://doi.org/10.1175/JCLI3366.1

        Note
        ----
        Only little of the code is specific to calculate the
        SMdd threshold - most could be re-used for other Zhang-like
        threshold estimates. However, it is super slow...
        """

        if beg >= end:
            raise ValueError()

        self.var = var
        self.quantile = quantile
        self.beg = beg
        self.end = end
        self.dim = dim
        self._name = f"SM_dry_days_clim_c{beg}-{end}_q{quantile}"

    def _trans(self, da, attrs):

        dim = self.dim
        beg = self.beg
        end = self.end

        # subset the climatology
        da = da.sel(**{dim: slice(str(beg), str(end))})

        if len(da[self.dim]) == 0:
            return [], attrs

        # is slow even when loaded
        da = da.load()

        # create the output array
        thresh_full = xr.full_like(da, np.NaN)

        for year in range(beg, end + 1):

            # exclude the year we calculate the threshold for
            a = da.sel(time=da[dim].dt.year != year)

            # select a random year (excluding the one we are interested in)
            years = np.unique(a[dim].dt.year)
            random_year = np.random.choice(years, 1).item()
            b = da.sel({dim: str(random_year)})

            # combine them
            da_subset = xr.concat([a, b], dim=dim)

            # calculate the monthly quantile
            da_subset = da_subset.groupby(f"{dim}.month")
            # using skipna=False is *much* faster
            thresh_res = da_subset.quantile(0.1, dim=dim, skipna=False)

            # assign the monthly threshold to every day of a month
            sel_year = da[dim].dt.year == year
            for month in range(1, 13):
                sel_month = da[dim].dt.month == month
                sel = sel_year & sel_month

                thresh_full.loc[{dim: sel}] = thresh_res.sel(month=month).values

        return thresh_full, attrs


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

        if not xru.alignable(da, threshold):
            raise ValueError("not alignable")

        da = da.groupby(f"{self.dim}.month")

        # select the variable from threshold, else we end up with a Dataset,
        # as threshold is from a climatology of the same data can use self.var
        threshold = threshold[self.var]

        # find days where SM is below the threshold
        da = da < threshold

        # count the number of days per year
        da = da.resample(**{self.dim: "A"}).sum()

        return da, attrs
