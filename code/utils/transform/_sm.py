import numpy as np
import xarray as xr

from .. import xarray_utils as xru
from .utils import _ProcessWithXarray


class SM_dry_days_clim_Zhang(_ProcessWithXarray):
    def __init__(self, var, quantile=0.1, beg=1850, end=1900, dim="time", mask=None):
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
        Only little of the code is specific to calculate the SMdd threshold - most could
        be re-used for other Zhang-like threshold estimates. However, it is super slow
        for daily data...
        """

        if beg >= end:
            raise ValueError()

        self.var = var
        self.quantile = quantile
        self.beg = beg
        self.end = end
        self.dim = dim
        self.mask = mask

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


class _SM_dry_days_Zhang_(_ProcessWithXarray):
    def __init__(self, var, threshold, is_pic, dim="time", freq="A", mask=None):
        """calc climatology of SM dry days after Zhang 2005

        Parameters
        ----------
        var : str
            Name of the variable on the Dataset
        threshold : xr.DataArray
            DataArray containing the threshold derived with
            SM_dry_days_clim_Zhang
        is_pic : bool
            Whether da is a piControl simulation or not. If it is not
            treats the climatological period different than the other
            period(s).

        References
        ----------
        https://doi.org/10.1175/JCLI3366.1
        https://link.springer.com/article/10.1007/s00382-007-0340-z

        Note
        ----
        Only little of the code is specific to calculate the SMdd threshold - most could
        be re-used for other Zhang-like threshold estimates. However, it is super slow
        for daily data...
        """

        self.var = var
        self.threshold = threshold
        self.is_pic = is_pic
        self.dim = dim
        self.freq = freq
        self.mask = mask

        if len(threshold) == 0:
            self.beg = self.end = "none"
        else:
            # calculate the start and end year from the threshold
            self.years = threshold[dim].dt.year
            self.beg = self.years.min().item()
            self.end = self.years.max().item()

        self._name = f"SM_dry_days_c{self.beg}-{self.end}_q"

    def _trans(self, da, attrs):

        # unpack some variables
        dim = self.dim
        dim_month = f"{dim}.month"
        self.dim_month = dim_month
        threshold = self.threshold

        if len(da) == 0 or len(threshold) == 0:
            return [], attrs

        threshold = threshold[self.var]

        # is a piControl simulation: we can process the whole da at once
        if self.is_pic:
            thresh_monthly = threshold.groupby(dim_month).mean()
            return self.statistic(da, thresh_monthly, True), attrs

        # is not a piControl simulation -> need to check if there's time overlap
        # with the threshold

        # split the dataset into 3 parts
        before_clim = da.sel({dim: slice(None, str(self.beg - 1))})
        during_clim = da.sel({dim: slice(str(self.beg), str(self.end))})
        after_clim = da.sel({dim: slice(str(self.end + 1), None)})

        if len(before_clim[dim]) or len(after_clim[dim]):
            thresh_monthly = threshold.groupby(dim_month).mean()

        out = list()

        if len(before_clim[dim]):
            before_clim = self.statistic(before_clim, thresh_monthly, True)
            out.append(before_clim)

        if len(during_clim[dim]):
            during_clim = self.statistic(during_clim, threshold, False)
            out.append(during_clim)

        if len(after_clim[dim]):
            after_clim = self.statistic(after_clim, thresh_monthly, True)
            out.append(after_clim)

        out = xr.concat(out, dim=dim)

        return out, attrs


class SM_dry_days_Zhang(_SM_dry_days_Zhang_):
    def statistic(self, da_, thresh, groupby):
        da_ = da_.groupby(self.dim_month) if groupby else da_
        return (da_ < thresh).resample({self.dim: self.freq}).sum()


class SM_dry_days_Intensity_Zhang(_SM_dry_days_Zhang_):
    def statistic(self, da_, thresh, groupby):
        da_ = da_.groupby(self.dim_month) if groupby else da_
        # remove the threshold and calculate the mean over everything that is < 0
        da_ = da_ - thresh
        da_ = da_.where(da_ < 0)
        return da_.resample({self.dim: self.freq}).mean(skipna=True)


class SM_dry_days_clim(_ProcessWithXarray):
    def __init__(
        self, var, quantile=0.1, clim=slice("1850", "1900"), dim="time", mask=None
    ):
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
        self.mask = mask

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
    def __init__(self, var, dim="time", mask=None):
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
        self.mask = mask

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
