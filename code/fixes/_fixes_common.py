import cftime
import xarray as xr


def _remove_matching_fN(fNs, *files_to_remove):
    """remove matching file names from a list

    Parameters
    ----------
    fNs : list of str
        list of filenames
    files_to_remove : str
        file names to remove in the list

    Returns
    -------
    fNs : list of str
        list of filenames

    """

    for file_to_remove in files_to_remove:
        fNs = [fN for fN in fNs if file_to_remove not in fN]

    return fNs


def _remove_non_matching_fN(fNs, *files_to_keep):
    """remove non-matching file names from a list

    Parameters
    ----------
    fNs : list of str
        list of filenames
    files_to_keep : str
        file names to keep in the list

    Returns
    -------
    fNs : list of str
        list of filenames
    """

    return [fN for fN in fNs if any([f_keep in fN for f_keep in files_to_keep])]


def _corresponds_to(meta, **conditions) -> bool:
    """check if metadata correspods to all the conditions

    Parameters
    ----------
    meta : dict
        Dictionary of metadata, e.g. {"model": "a", "exp": "b", ...}.
    conditions : Mapping from the keys to the conditions.

    Notes
    -----
    - individual conditions are combined with "and", i.e. `conditions = {"model": "a",
      "exp": "b"}` requires the model to be "a" and the experiment to be "b".
    - listed conditions for a key are combined with "or", i.e. `conditions = {"model":
      ["a", "b"]}` matches for both.
    """

    # make sure conditions is always a list
    for key, value in conditions.items():
        if isinstance(value, str):
            conditions[key] = [value]

    return all(meta[key] in cond for key, cond in conditions.items())


def _maybe_rename(ds, name, target, candidates):
    """rename coord/ dim if it is in the dataset

    Parameters
    ----------
    ds : xr.Dataset
        Dataset whose dims and coords should be renamed
    name : str
        Name of the coord/ dim to potentally rename
    target : str
        Desired name
    candidates : list of str
        Names that should be changed

    """

    if name in candidates:
        ds = ds.rename({name: target})
    return ds


def unify_coord_names(ds):
    """make sure lat and lon coord names are unified and consistent

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to unify the coordinates over

    Notes
    -----
    - 2D coordinates (of ocean files) are named latitude and longitude
    - 1D coordinates are named lat and lon
    """

    # all dims
    dims = set(ds.dims)
    # no dimension coordinates (i.e. the 2D coords)
    no_dim_coords = set(ds.coords) - dims

    # no_dim_coords (=2D coords) must be first
    for no_dim_coord in no_dim_coords:
        ds = _maybe_rename(ds, no_dim_coord, "longitude", ["lon", "nav_lon"])
        ds = _maybe_rename(ds, no_dim_coord, "latitude", ["lat", "nav_lat"])

    for dim in dims:
        ds = _maybe_rename(ds, dim, "lon", ["x", "i", "ni", "xh", "nlon", "longitude"])
        ds = _maybe_rename(ds, dim, "lat", ["y", "j", "nj", "yh", "nlat", "latitude"])

    return ds


def data_vars_as_coords(ds):
    """set data variables as coordinates

    Notes
    -----
    - at least on model had coordinates as data_vars
    """

    candidates = ["lat", "lon", "lon_bounds", "lat_bounds"]

    for candidate in candidates:
        if candidate in ds.data_vars:
            ds = ds.set_coords(candidate)

    return ds


def delete_bounds(ds, dim: str):
    """delete bounds of coordinates

    Parameters
    ----------
    ds : xr.Dataset
        Dataset to remove the bounds from
    dim : str
        Dimension name to remove the bounds from
    """

    # delete latitude bounds
    if dim in ds.dims and "bounds" in ds[dim].attrs:
        bounds_name = ds[dim].attrs["bounds"]
        del ds[bounds_name]
        del ds[dim].attrs["bounds"]

    return ds


def convert_time_to_proleptic_gregorian(ds, dim="time"):
    """convert time index to ProlepticGregorian calendar

    Parameters
    ----------
    ds : xr.Dataset
        Dataset to convert the calendar for.
    dim : str, default: "time"
        Name of the time dimension.
    """

    time = ds.indexes[dim]

    time = [
        cftime.DatetimeProlepticGregorian(
            t.year, t.month, t.day, t.hour, t.minute, t.second
        )
        for t in time
    ]

    time = xr.CFTimeIndex(time)

    return ds.assign_coords({dim: time})


def convert_time_to(ds, new_calendar, dim="time"):
    """convert time index to ProlepticGregorian calendar

    Parameters
    ----------
    ds : xr.Dataset
        Dataset to convert the calendar for.
    new_calendar : str
        New calendar to encode the time index to.
    dim : str, default: "time"
        Name of the time dimension.
    """
    # convert the time back to the original
    num, units, old_calendar = xr.coding.times.encode_cf_datetime(ds[dim])

    # decode with the new calendar
    time = xr.coding.times.decode_cf_datetime(num, units, new_calendar, use_cftime=True)

    return ds.assign_coords({dim: time})


def fixes_common(ds):
    """
    Apply fixes that may apply to all datasets.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to fix.

    Returns
    -------
    ds : same as input
    """

    # delete height
    if "height" in ds.variables:
        del ds["height"]

    # fix coords status and names (lat & lon)
    ds = data_vars_as_coords(ds)
    ds = unify_coord_names(ds)

    ds = delete_bounds(ds, "lat")
    ds = delete_bounds(ds, "lon")
    ds = delete_bounds(ds, "x")
    ds = delete_bounds(ds, "y")
    ds = delete_bounds(ds, "time")

    return ds
