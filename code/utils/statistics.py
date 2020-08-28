import numpy as np
import scipy as sp
import statsmodels.api as sm
import xarray as xr


def mannwhitney(d1, d2, alpha=0.05, stack=("lat", "lon")):
    """Wilcoxon–Mann–Whitney-U test with Benjamini and Hochberg correction"""

    # make lat/ lon a 1D variable
    d1_stack = d1.stack(stacked=stack)
    d2_stack = d2.stack(stacked=stack)

    # create dummy array to store the results
    dims = set(d1_stack.dims) - set(["stacked"])
    result = d1_stack.mean(dims)

    for i in range(result.stacked.shape[0]):

        # unpack ens/ time
        v1 = d1_stack.isel(stacked=i).values.ravel()
        v2 = d2_stack.isel(stacked=i).values.ravel()

        # only calculate if we actually have data
        if (~np.isnan(v1)).sum() > 0:
            _, p_val = sp.stats.mannwhitneyu(v1, v2)
        else:
            p_val = 1.0

        result[i] = p_val

    result = result.unstack("stacked")

    # apply Benjamini and Hochberg correction
    shape = result.shape
    p_adjust = sm.stats.multipletests(
        result.values.ravel(), alpha=alpha, method="fdr_bh"
    )[0]
    p_adjust = p_adjust.reshape(shape)

    result.values[:] = p_adjust

    return result


MANNWHITNEY_DICT = dict()


def get_mannwhitney(d1, d2, name):
    # cache the results of mannwhitney, as it is slow

    if name not in MANNWHITNEY_DICT.keys():

        mw = mannwhitney(d1, d2)

        MANNWHITNEY_DICT[name] = mw

    return MANNWHITNEY_DICT[name]


def mannwhitney_ufunc(da1, da2, dim="time", alpha=0.05):
    def mannwhitney_(v1, v2):

        # return NaN for all-nan vectors
        if np.isnan(v1).all() or np.isnan(v2).all():
            return np.NaN

        # use masked-stats if any is nan
        if np.isnan(v1).any() or np.isnan(v2).any():
            v1 = np.ma.masked_invalid(v1)
            v2 = np.ma.masked_invalid(v2)

            _, p_val = sp.stats.mstats.mannwhitneyu(v1, v2)

        else:
            _, p_val = sp.stats.mannwhitneyu(v1, v2)

        return p_val

    dim = [dim] if isinstance(dim, str) else dim

    # use xr.apply_ufunc to handle vectorization
    result = xr.apply_ufunc(
        mannwhitney_,
        da1,
        da2,
        input_core_dims=[dim, dim],
        output_core_dims=[[]],
        exclude_dims=set(dim),
        vectorize=True,
        dask="parallelized",
        output_dtypes=[float],
    ).compute()

    # apply Benjamini and Hochberg correction
    shape = result.shape
    p_adjust = sm.stats.multipletests(
        result.values.ravel(), alpha=alpha, method="fdr_bh"
    )[0]
    p_adjust = p_adjust.reshape(shape)

    result.values[:] = p_adjust

    return result


def theil_ufunc(da, dim="time", alpha=0.1):
    """theil sen slope for xarray

        Wraps sp.stats.theilslopes in xr.apply_ufunc

        Parameters
        ==========
        da : xr.DataArray
            DataArray to calculate the theil sen slope over
        dim : list of str, optional
            Dimensions to reduce the array over. Default: "time"
        alpha : float, optional
            Significance level in [0, 0.5].

        Returns
        =======
        slope : xr.DataArray
            Median slope of the array
        significance : xr.DataArray
            Array indicating significance. True if significant,
            False otherwise
    """

    def theil_(pt, alpha):

        # return NaN for all-nan vectors
        if np.isnan(pt).all():
            return np.NaN, np.NaN

        # use masked-stats if any is nan
        if np.isnan(pt).any():
            pt = np.ma.masked_invalid(pt)
            slope, inter, lo_slope, up_slope = sp.stats.mstats.theilslopes(
                pt, alpha=alpha
            )

        else:
            slope, inter, lo_slope, up_slope = sp.stats.theilslopes(pt, alpha=alpha)

        # theilslopes does not return siginficance but a
        # confidence intervall assume it is significant
        # if both are on the same side of 0
        significance = np.sign(lo_slope) == np.sign(up_slope)

        return slope, significance

    kwargs = dict(alpha=alpha)
    dim = [dim] if isinstance(dim, str) else dim

    # use xr.apply_ufunc to handle vectorization
    theil_slope, theil_sign = xr.apply_ufunc(
        theil_,
        da,
        input_core_dims=[dim],
        vectorize=True,
        output_core_dims=((), ()),
        kwargs=kwargs,
    )

    return theil_slope, theil_sign
