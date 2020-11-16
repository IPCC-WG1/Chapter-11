import glob

import numpy as np
import xarray as xr

from .file_utils import _any_file_does_not_exist


def _read_data_for_postprocess(
    fNs_in_or_creator, metadata, fixes=None, fixes_preprocess=None,
):

    # get fNs_in
    if isinstance(fNs_in_or_creator, (list, str)):
        fNs_in = fNs_in_or_creator
    else:
        fNs_in = fNs_in_or_creator(metadata)

    # exit if the model is removed by the fixes
    if fNs_in is None:
        return [], []

    # postprocess
    # try:

    ds = mf_read_netcdfs(
        fNs_in, metadata=metadata, fixes=fixes, fixes_preprocess=fixes_preprocess,
    )

    return fNs_in, ds


def postprocess(
    fN_out,
    fNs_in_or_creator,
    metadata,
    transform_func=None,
    fixes=None,
    fixes_preprocess=None,
    **kwargs
):
    """ postprocessing-on-the-fly and loading function

    """

    var = metadata.get("varn", None)
    var_out = kwargs.pop("var_out", None)

    if var_out is None:
        var_out = var

    # if fN_out does not exits, create it on the fly
    if _any_file_does_not_exist(fN_out):

        fNs_in, ds = _read_data_for_postprocess(
            fNs_in_or_creator, metadata, fixes=fixes, fixes_preprocess=fixes_preprocess
        )

        # ds = _maybe_load_additonal_data()

        # except Exception as e:
        #     print("ERROR\n\n\n")
        #     print(str(e))
        #     print("ERROR\n\n\n")
        #     return []

        if transform_func is not None:
            ds = transform_func(ds)

        if len(ds) == 0:
            return []

        if isinstance(ds, xr.DataArray):
            ds = ds.to_dataset(name=var_out)

        # add source files
        
        fNs_in = [fNs_in] if isinstance(fNs_in, str) else fNs_in
        ds.attrs["source_files"] = ", ".join(fNs_in)

        # fix for: https://github.com/pydata/xarray/issues/3665
        if "time" in ds:
            ds.time.encoding.pop("_FillValue", None)

        # save as netcdf
        ds.to_netcdf(fN_out, format="NETCDF4_CLASSIC")

        return ds

    else:
        pass
        # return xr.open_dataset(fN_out, use_cftime=True)


def mf_read_netcdfs(
    files, metadata, fixes=None, fixes_preprocess=None,
):

    ds = xr.open_mfdataset(
        files,
        concat_dim="time",
        combine="by_coords",
        coords="minimal",
        data_vars="minimal",
        compat="override",
        parallel=False,
        decode_cf=True,
        use_cftime=True,
        preprocess=fixes_preprocess,
    )

    # get rid of the "days" units, else CDD will have dtype = timedelta
    varn = metadata["varn"]
    units = ds[varn].attrs.get("units", None)
    if units in ["seconds", "days"]:
        ds[varn].attrs.pop("units")

    # ds = xr.decode_cf(ds, use_cftime=True)

    if fixes is not None:
        ds = fixes(ds, metadata, None)

    return ds


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

    def process_one_path(path, next_path):

        # use a context manager, to ensure the file gets closed after use
        with xr.open_dataset(path, **kwargs) as ds:

            if fixes is not None:
                ds = fixes(ds, metadata, next_path)

            if ds is None:
                return None

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

    datasets = list()

    for i, path in enumerate(paths):
        # get the next path in line (if there is one)
        next_path = None if i + 1 == len(paths) else paths[i + 1]

        ds = process_one_path(paths[i], next_path)

        if ds is not None:
            datasets.append(ds)

    combined = xr.concat(datasets, dim, compat="override", coords="minimal")

    return combined


# =============================================================================


def cos_wgt(ds, lat="lat"):
    """cosine-weighted latitude"""
    return np.cos(np.deg2rad(ds[lat]))


# =============================================================================
