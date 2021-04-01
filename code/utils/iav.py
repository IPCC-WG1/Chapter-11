from collections import namedtuple

import numpy as np
import xarray as xr

from . import computation, plot


def _ensure_same(da, da_abs):
    def _cmp(equal_, msg):
        if not equal_:
            raise ValueError(f"Different {msg}!")

    if not isinstance(da, xr.DataArray):
        raise ValueError(f"'da' is not a DataArray, found {type(da)}")

    if not isinstance(da_abs, xr.DataArray):
        raise ValueError(f"'da_abs' is not a DataArray, found {type(da_abs)}")

    _cmp(da.shape == da_abs.shape, "shapes")
    _cmp(da.name == da_abs.name, "names")
    _cmp(da.model.equals(da_abs.model), "model")
    _cmp(da.exp.equals(da_abs.exp), "exp")
    _cmp(da.ens.equals(da_abs.ens), "ens")


data_iav_aligned = namedtuple(
    "data_iav_aligned", ["da", "iav", "da_abs", "iav_used_models"]
)


def align_for_iav(iav, da, da_abs=None):
    """align iav and DataArray for plotting

    Parameters
    ----------
    iav : datalist
        Interannual variability estimates to align
    da : DataArray
        Absolute anomalies to align (and eventually to estimate significance & maybe to
        plot).
    da_abs : DataArray, optional
        Aboslute anomalies to align and to estimate significance if da is relative (%,
        z-score)

    Returns
    -------
    DataArrays : aligned da's
    """

    # try to make sure da and da_abs have the same coords (except for the anomalies)
    if da_abs is not None:
        _ensure_same(da, da_abs)

    varn = iav[0][1]["varn"]
    iav = computation.concat_xarray_with_metadata(iav)[varn]

    isel_da = list()
    sel_iav = list()

    # go through all models in da
    for i, model in enumerate(da.model.values):

        # check if a piControl simulation exists
        if model in iav.model:

            isel_da.append(i)
            sel_iav.append(model)

    # only use models with a corresponding piControl simulation
    da = da.isel(mod_ens=isel_da)

    # we cannot sel by a non-dimension coordinate
    # swap dims -> select -> swap back
    iav_ = iav.swap_dims({"mod_ens": "model"})
    iav_used_models = iav_.sel(model=np.unique(sel_iav))
    iav_ = iav_.sel(model=sel_iav)
    iav_ = iav_.swap_dims({"model": "mod_ens"})

    if da_abs is not None:
        da_abs = da_abs.isel(mod_ens=isel_da)
        return data_iav_aligned(
            da=da, iav=iav_, da_abs=da_abs, iav_used_models=iav_used_models
        )

    return data_iav_aligned(
        da=da, iav=iav_, da_abs=None, iav_used_models=iav_used_models
    )


def _get_same_sign(da, thresh_samesign, dim="mod_ens"):
    """calculate if 'thresh_samesign' simulations have the same sign

    Parameters
    ----------
    ds : xr.Dataset
        data to check sign
    thresh_samesign : float
        threshold for the fraction of models that need to have the same sign
    dim : str
        Name of the ensemble dimension.

    Returns
    -------
    consistent_change : xr.DataArray
        boolean array whether more than thresh_samesign simulations
        have the same sign
    """

    if not (thresh_samesign > 0) and (thresh_samesign < 1):
        raise ValueError(f"thresh must be in 0..1, is {thresh_samesign}")

    # n_ens = len(da[dim])

    notnull = da.notnull()

    n_ens = notnull.sum(dim)

    # get the sign of change
    sign = np.sign(da)

    # calculate fraction of models with same pos/ neg sign
    # and check if more than thresh fulfill have same sign
    pos_change = ((sign == 1).sum(dim) / n_ens) >= thresh_samesign
    neg_change = ((sign == -1).sum(dim) / n_ens) >= thresh_samesign

    # it needs to either be neg or pos
    consistent_change = neg_change | pos_change

    return consistent_change


def _get_signif_indiv(da, iav, n_sigma, thresh_signif, dim="mod_ens"):
    """calculate if 'thresh_signif' simulations are significant

    Parameters
    ----------
    ds : xr.Dataset
        data to check significance
    iav : xr.Dataset
        Inter-annual variability
    thresh_signif : float
        threshold for the fraction of models that need to be
        significant
    dim : str
        Name of the ensemble dimension.

    Returns
    -------
    enough_signif : xr.DataArray
        boolean array whether this gridpoint is significant

    Note
    ----
    ds and iav must be aligned
    """

    if not (thresh_signif > 0) and (thresh_signif < 1):
        raise ValueError(f"thresh must be in 0..1, is {thresh_signif}")

    if iav.min() < 0:
        raise ValueError("iav cannot be negative!")

    n_ens = len(da[dim])

    # iav is positive per definition, we are interested in
    # a two sided test
    da = np.abs(da)

    enough_signif = ((da > iav * n_sigma).sum(dim) / n_ens) >= thresh_signif

    return enough_signif


N_SIGMA = np.sqrt(2) * 1.645  # for cmip with piControl simulations


def add_signif_sign_hatch(
    ax, da, iav, n_sigma=N_SIGMA, thresh_signif=0.66, thresh_samesign=0.8, dim="mod_ens"
):
    """
    add hatching given the signal and the interannual variability
    """

    # minimal check that da and iav are aligned
    # use .variable -> we don't want to compare non-dim coords
    if not da.model.variable.equals(iav.model.variable):
        raise ValueError("must be aligned")

    consistent_change = _get_same_sign(da, thresh_samesign, dim=dim)
    signif = _get_signif_indiv(da, iav, n_sigma, thresh_signif, dim)

    # we don't want to hatch Na
    all_notnull = da.notnull().all(dim)

    h0 = plot.text_legend(ax, "Color", "Robust signal", size=7)

    label = "Conflicting signals"
    cond = signif & ~consistent_change & all_notnull
    h1 = plot.hatch_map(ax, cond, 8 * "X", label=label)

    # label = "No (robust) change"

    label = "No change or no robust signal"
    cond = ~signif & all_notnull
    h2 = plot.hatch_map(ax, cond, 8 * "\\", label=label)

    return [h0, h1, h2]
