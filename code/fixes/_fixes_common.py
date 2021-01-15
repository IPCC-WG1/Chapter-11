import cftime
import xarray as xr


def _remove_matching_fN(fNs, *files_to_remove):

    for file_to_remove in files_to_remove:
        fNs = [i for i in fNs if file_to_remove not in i]

    return fNs


def _remove_non_matching_fN(fNs, *files_to_keep):

    return [fN for fN in fNs if any([f_keep in fN for f_keep in files_to_keep])]


def _corresponds_to(metadata, **conditions):

    for key, value in conditions.items():
        if isinstance(value, str):
            conditions[key] = [value]

    return all(metadata[key] in cond for key, cond in conditions.items())


def _maybe_rename(ds, name, target, candidates):
    """

    Parameters
    ==========
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

    dims = set(ds.dims)
    no_dim_coords = set(ds.coords) - dims

    # no_dim_coords must be first
    for no_dim_coord in no_dim_coords:
        ds = _maybe_rename(ds, no_dim_coord, "longitude", ["lon", "nav_lon"])
        ds = _maybe_rename(ds, no_dim_coord, "latitude", ["lat", "nav_lat"])

    for dim in dims:
        ds = _maybe_rename(ds, dim, "lon", ["x", "i", "ni", "xh", "nlon", "longitude"])
        ds = _maybe_rename(ds, dim, "lat", ["y", "j", "nj", "yh", "nlat", "latitude"])

    return ds


def data_vars_as_coords(ds):
    """at least on model had coordinates as data_vars"""

    candidates = ["lat", "lon", "lon_bounds", "lat_bounds"]

    for candidate in candidates:
        if candidate in ds.data_vars:
            ds = ds.set_coords(candidate)

    return ds


def convert_time_to_proleptic_gregorian(ds, dim="time"):

    time = ds.indexes["time"]

    time = [
        cftime.DatetimeProlepticGregorian(
            t.year, t.month, t.day, t.hour, t.minute, t.second
        )
        for t in time
    ]

    time = xr.CFTimeIndex(time)

    return ds.assign_coords({dim: time})


def convert_time_to(ds, new_calendar, dim="time"):

    # convert the time back to the original
    num, units, old_calendar = xr.coding.times.encode_cf_datetime(ds[dim])

    time = xr.coding.times.decode_cf_datetime(num, units, new_calendar, use_cftime=True)

    return ds.assign_coords({dim: time})


def fixes_common(ds):
    """
    Apply fixes that may apply to all datasets.

    Parameters
    ----------
    ds : xarray.Dataset
    metadata : dictionary

    Returns
    -------
    ds : same as input
    open_issues : list
        List of strings describing not fixable issues
    applies_fixes : string
        String of a semicolon-separated short description of the changes
    flag : int
    """

    # delete height
    if "height" in ds.variables:
        del ds["height"]

    ds = data_vars_as_coords(ds)
    ds = unify_coord_names(ds)

    # delete latitude bounds (#g1)
    if "lat" in ds.dims and "bounds" in ds["lat"].attrs:
        bounds_name = ds["lat"].attrs["bounds"]
        del ds[bounds_name]
        del ds["lat"].attrs["bounds"]

    # delete longitude bounds
    if "lon" in ds.dims and "bounds" in ds["lon"].attrs:
        bounds_name = ds["lon"].attrs["bounds"]
        del ds[bounds_name]
        del ds["lon"].attrs["bounds"]

    # delete x bounds
    if "x" in ds.dims and "bounds" in ds["x"].attrs:
        bounds_name = ds["x"].attrs["bounds"]
        del ds[bounds_name]
        del ds["x"].attrs["bounds"]

    # delete y bounds
    if "y" in ds.dims and "bounds" in ds["y"].attrs:
        bounds_name = ds["y"].attrs["bounds"]
        del ds[bounds_name]
        del ds["y"].attrs["bounds"]

    # delete time bounds (#g2)
    if "time" in ds.dims and "bounds" in ds["time"].attrs:
        bounds_name = ds["time"].attrs["bounds"]
        del ds[bounds_name]
        del ds["time"].attrs["bounds"]

    return ds


# DRAFT:
# def add_year_of_data(ds, year_template, where, delta_days):

#         temp = ds.sel(time=slice('2099', '2099'))
#         temp *= np.nan

#         temp['time'].data += timedelta(days=delta_days)

#         if where == "before":
#             ds = xr.concat([temp, ds], dim='time')
#         elif: where == "after"
#             ds = xr.concat([ds, temp], dim='time')
#         else:
#             raise ValueError("'where' must be one of 'before' and 'after'")

#         return ds
