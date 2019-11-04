def fixes_common(ds, metadata):
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
    if "height" in ds.dims:
        del ds["height"]

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