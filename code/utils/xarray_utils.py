import glob

import numpy as np
import xarray as xr

from .file_utils import _any_file_does_not_exist


def postprocess(fN_out, fNs_in, metadata, transform_func=None, fixes=None, **kwargs):
    """ postprocessing-on-the-fly and loading function

    """

    var = metadata.get("varn", None)

    var_out = kwargs.pop("var_out", None)
    dim = kwargs.pop("dim", "time")

    if var_out is None:
        var_out = var

    # if fN_out does not exits, create it on the fly
    if _any_file_does_not_exist(fN_out):

        # postprocess
        ds = read_netcdfs(
            fNs_in,
            dim=dim,
            metadata=metadata,
            transform_func=transform_func,
            fixes=fixes,
            **kwargs
        )

        if ds is None:
            return []

        if isinstance(ds, xr.DataArray):
            ds = ds.to_dataset(name=var_out)

        # add source files
        ds.attrs["source_files"] = ", ".join(fNs_in)

        # save as netcdf
        ds.to_netcdf(fN_out, format="NETCDF4_CLASSIC")

        return ds

    else:
        return xr.open_dataset(fN_out, use_cftime=True)


def read_netcdfs(files, dim, metadata, transform_func=None, fixes=None, **kwargs):
    """
    read and combine multiple netcdf files

    Parameters
    ----------
    files : string or list of files
        path with wildchars or iterable of files
    dim : string
        dimension along which to combine, does not have to exist in
        file (e.g. ensemble)
    transform_func : function
        function to apply for individual datasets, see example
    kwargs : keyword arguments
        passed to open_dataset

    Returns
    -------
    combined : xarray Dataset
        the combined xarray Dataset with transform_func applied

    Example
    -------
    read_netcdfs('/path/*.nc', dim='ens',
                 transform_func=lambda ds: ds.mean())

    Reference
    ---------
    http://xarray.pydata.org/en/stable/io.html#combining-multiple-files
    """

    def process_one_path(path):

        # use a context manager, to ensure the file gets closed after use
        with xr.open_dataset(path, **kwargs) as ds:

            if fixes is not None:
                ds = fixes(ds, metadata)

            # transform_func should do some sort of selection or
            # aggregation
            if transform_func is not None:
                ds = transform_func(ds)

            # load all data from the transformed dataset, to ensure we can
            # use it after closing each original file
            ds.load()
            return ds

    kwargs["use_cftime"] = kwargs.pop("use_cftime", True)

    if isinstance(files, str):
        paths = sorted(glob.glob(files))
    else:
        paths = sorted(files)

    datasets = [process_one_path(p) for p in paths]

    combined = xr.concat(datasets, dim, compat="override", coords="minimal")

    return combined


# =============================================================================


def cos_wgt(ds, lat="lat"):
    """cosine-weighted latitude"""
    return np.cos(np.deg2rad(ds[lat]))


# =============================================================================


def _average_da(da, dim=None, axis=None, weights=None, skipna=None, keep_attrs=False):
    """
    weighted average for DataArrays

    Parameters
    ----------
    dim : str or sequence of str, optional
        Dimension(s) over which to apply `average`.
    axis : int or sequence of int, optional
            Axis(es) over which to apply `average`. Only one of the 'dim'
            and 'axis' arguments can be supplied. If neither are supplied, then
            `average` is calculated over axes.
    weights : DataArray
        weights to apply. Shape must be broadcastable to shape of da.
    keep_attrs : bool, optional
        If True, the attributes (`attrs`) will be copied from the original
        object to the new one.  If False (default), the new object will be
        returned without attributes.

    Returns
    -------
    reduced : DataArray
        New DataArray with average applied to its data and the indicated
        dimension(s) removed.

    """

    if weights is None:
        return da.mean(dim=dim, axis=axis, keep_attrs=keep_attrs)

    if not isinstance(weights, xr.DataArray):
        raise TypeError("weights must be a DataArray")

    valid = da.notnull()
    sum_of_weights = weights.where(valid).sum(dim=dim, axis=axis)

    # replace 0. weights with NaN
    sum_of_weights = sum_of_weights.where(sum_of_weights != 0.0)

    weighted_sum = (da * weights).sum(
        dim=dim, axis=axis, skipna=skipna, keep_attrs=keep_attrs
    )

    return weighted_sum / sum_of_weights


def _average_ds(ds, dim=None, axis=None, weights=None, skipna=None, keep_attrs=False):
    """
    weighted average for Datasets

    Parameters
    ----------
    dim : str or sequence of str, optional
        Dimension(s) over which to apply `average`.
    axis : int or sequence of int, optional
        Axis(es) over which to apply `average`. Only one of the 'dim'
        and 'axis' arguments can be supplied. If neither are supplied, then
        `average` is calculated over axes.
    weights : Dataset, optional
        An array of weights associated with the values in this Dataset.
        Each value in a contributes to the average according to its
        associated weight. The weights array can either be 1-D (in which
        case its length must be the size of a along the given axis or
        dimension) or of he same shape this Dataset. If weights=None, then
        all data in this Dataset are assumed to have a weight equal to one.
    keep_attrs : bool, optional
        If True, the attributes (`attrs`) will be copied from the original
        object to the new one.  If False (default), the new object will be
        returned without attributes.
    Returns
    -------
    reduced : Dataset
        New Dataset with average applied to its data and the indicated
        dimension(s) removed.

    """

    if weights is None:
        return ds.mean(dim=dim, axis=axis, keep_attrs=keep_attrs)
    else:
        return ds.apply(_average_da, dim=dim, axis=axis, weights=weights)


def average(obj, dim=None, axis=None, weights=None, skipna=None, keep_attrs=False):

    if isinstance(obj, xr.DataArray):

        return _average_da(
            obj,
            dim=dim,
            axis=axis,
            weights=weights,
            skipna=skipna,
            keep_attrs=keep_attrs,
        )

    elif isinstance(obj, xr.Dataset):

        return _average_ds(
            obj,
            dim=dim,
            axis=axis,
            weights=weights,
            skipna=skipna,
            keep_attrs=keep_attrs,
        )

    raise TypeError("obj must be 'DataArray' or 'Dataset'")
