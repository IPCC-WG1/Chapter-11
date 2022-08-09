import warnings

import numpy as np
import xarray as xr


def open_mfdataset(
    fNs_in,
    metadata,
    fixes=None,
    fixes_preprocess=None,
    check_time=True,
):
    """wrapper for xarray.open_mfdataset to read cmip6 data

    Parameters
    ----------
    fNs_in : list of str
        Filenames to read and concatenate.
    metadata : dict
        Metadata dictionary containing information on the files to read.
    fixes : Callable
        Function to fix data after concatenation. See also the fixes folder (../fixes).
    fixes_preprocess : Callable
        Function to fix data before concatenation. See also the preprocess keyword of
        ``xr.open_mfdataset`` and the fixes folder (../fixes).
    check_time : bool, default: True
        If True checks the time axis for missing time step.

    Returns
    -------
    out : xr.Dataset
        Loaded netCDFs.
    """

    # inatialize fixes_preprocess
    fixes_preprocess = fixes_preprocess(metadata, fNs_in)

    ds = xr.open_mfdataset(
        fNs_in,
        concat_dim="time",
        combine="by_coords",
        coords="minimal",
        data_vars="minimal",
        compat="override",
        parallel=False,
        decode_cf=True,
        use_cftime=True,
        preprocess=fixes_preprocess,
    ).load()

    # get rid of the "days" units, else CDD will have dtype = timedelta
    varn = metadata["varn"]
    units = ds[varn].attrs.get("units")
    if units in ["seconds", "days"]:
        ds[varn].attrs.pop("units")

    # ds = xr.decode_cf(ds, use_cftime=True)

    ds = remove_duplicated_timesteps(ds, dim="time")

    if fixes is not None:
        ds, time_check = fixes(ds, metadata)

    if time_check and check_time and "time" in ds.coords:
        if not all_years(ds, errors="warn"):
            return []
        assert_all_timesteps(ds)

    # float32 is not good enough...
    if ds[varn].dtype == np.float32:
        ds[varn] = ds[varn].astype(float)

        # add source files

    fNs_in = [fNs_in] if isinstance(fNs_in, str) else fNs_in
    ds.attrs["source_files"] = ", ".join(fNs_in)

    return ds


def cos_wgt(ds, lat="lat"):
    """cosine-weights of the latitude"""
    return np.cos(np.deg2rad(ds[lat]))


def all_years(ds, dim="time", errors="raise"):
    """check all years are present

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
        Data to check the time axis for.
    dim : str, default: "time"
        Name of the time coordinates. Datetime coordinates are expeced.
    errors : str, default: "raise"
        Behaviour on inclomplete years. Raise a ValueError if it is set to "raise" or
        a warning if "warn" is indicated.

    Returns
    -------
    all_years : bool
        Returns True if all years are present and False if not.

    Raises
    ------
    ValueError : if not all years are present and errors="raise".
    """

    if dim in ds.coords:
        year = ds[dim].dt.year
        first_year = year.min().item()
        last_year = year.max().item()

        n_years_expected = last_year - first_year + 1
        n_years_actual = len(np.unique(year))

        if not n_years_expected == n_years_actual:
            if errors == "raise":
                raise ValueError("Missing years!")
            elif errors == "warn":
                warnings.warn("Missing years! - Skipping the file.")
                return False
            else:
                raise ValueError("Wrong argument")
        return True


def assert_all_timesteps(ds, dim="time"):
    """check if time coordinates are 'equally' spaced

    Currently only works for daily and monthly data.

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
        Data to check the time axis for.
    dim : str, default: "time"
        Name of the time coordinates. Datetime coordinates are expeced.

    Raises
    ------
    ValueError : if missing time steps are detected
    """

    if dim in ds.coords:

        time = ds[dim]
        dt = time.diff(dim)
        delta_days = dt.dt.days
        dd0 = delta_days[0]

        # daily data
        if dd0 == 1:

            if (delta_days != 1).any():

                msg = "Problem for daily data:"

                delta_zero = delta_days == 0
                if delta_zero.any():
                    sum = delta_zero.sum().item()
                    msg += f" {sum} duplicated timesteps."

                delta_gt = delta_days > 1
                if delta_gt.any():
                    sum = delta_gt.sum().item()
                    mx = delta_days.max().item()
                    msg += f" {sum} hole(s) (max delta: {mx})."

                raise ValueError(msg)

        # monthly data
        elif (dd0 >= 28) and (dd0 <= 31):

            months_per_year = time.groupby("time.year").count()

            # be less strict with the first and last year
            if (months_per_year[1:-1] != 12).any():
                raise ValueError("Missing months")

            if months_per_year[0] != 12:
                warnings.warn("Incomplete first year")

            if months_per_year[-1] != 12:
                warnings.warn("Incomplete last year")

        else:
            raise ValueError("Unknwon time step")


def remove_duplicated_timesteps(ds, dim="time", max_allowed=5):
    """remove duplicated timesteps from dataset (if present)

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
        Data to remove duplicate timesteps from.
    dim : str, default: "time"
        Name of the time coordinates. Datetime coordinates are expeced.
    max_allowed : int, default: 5
        If more than ``max_allowed`` duplicate timesteps are found an error is raised.

    Returns
    -------
    ds : xr.Dataset or xr.DataArray
        Data with duplicate timesteps removed.

    Raises
    ------
    ValueError : if more than ``max_allowed`` duplicate timesteps are found
    """

    if dim in ds.coords:
        # find delta time in days
        time = ds[dim]
        delta_days = time.diff(dim).dt.days

        if (delta_days == 0).any():

            duplicates = delta_days.sel(time=delta_days == 0).time
            duplicates = np.unique(duplicates.values)

            n_duplicates = len(duplicates)

            if n_duplicates > max_allowed:
                raise ValueError(f"Found {n_duplicates} duplicated timesteps")

            warnings.warn(f"Found {n_duplicates} duplicated timestep(s)")

            # loop through all duplicates
            for duplicate in duplicates:

                sel = time == duplicate

                s = np.where(time == duplicate)[0][0]
                sel[s] = False

                ds = ds.sel({dim: ~sel})

    return ds


def alignable(*objects):
    """check if xr objects can be aligned"""

    try:
        xr.align(*objects, join="exact", copy=False)
        return True
    except ValueError:
        return False


def assert_alignable(*objects, message=""):
    """raise custom error message if xr objects are not alignable"""

    if not alignable(*objects):
        raise ValueError(message)


def maybe_reindex(da, target):
    """make sure da has the same coordinates as target

    Parameters
    ----------
    da : xr.Dataset or xr.DataArray
        Data to reindex like target.
    target : xr.Dataset or xr.DataArray
        Target to align to.

    Notes
    -----
    target can be smaller (a subset) than da.

    Returns
    -------
    da : xr.DataArray or xr.Dataset or None
        Aligned data. None is returned if alignment is not possible.
    """

    if alignable(da, target):
        return da

    # target has been selected
    subset = len(target.lat) < len(da.lat) or len(target.lon) < len(da.lon)

    # cannot compute allcose if it's a subset
    allclose = subset or (
        np.allclose(da.lat, target.lat) and np.allclose(da.lon, target.lon)
    )

    if allclose or subset:
        return da.reindex_like(target, method="nearest")

    return None


def check_range(
    da,
    min_allowed=None,
    max_allowed=None,
    min_smaller=None,
    max_larger=None,
    maybe_fix=True,
):
    """check if values are within the alowed range

    Parameters
    ----------
    da : xr.DataArray
        Data to check the values.
    min_allowed : float, default None
        Minimally allowed value. A ValueError is raised if smaller values are found. But
        see also ``maybe_fix``. If None no check is performed.
    max_allowed : float, default None
        Maximally allowed value. A ValueError is raised if larger values are found. But
        see also ``maybe_fix``. If None no check is performed.
    min_smaller : float, default None
        A ValueError is raised if the smallest value in da is larger than
        ``min_smaller``. If None no check is performed.
    max_lerger : float, default None
        A ValueError is raised if the largest value in da is smaller than
        ``max_larger``. If None no check is performed.
    maybe_fix : bool, default: True
        If True and the ``min_allowed`` or ``max_allowed`` conditions are only minimally
        violated (``np.allcose``) the data is fixed by setting it to the allowed values.
        This can fix numerical issues. If False no fix is attempted.

    Returns
    -------
    da : xr.DataArray
        Fixed input DataArray (if applicable).
    """

    # could replace None with +/- np.inf to simplify logic?

    if not isinstance(da, xr.DataArray):
        raise TypeError(f"must be a DataArray, found {type(da)}")

    mx = da.max().compute().item()
    mn = da.min().compute().item()

    if (min_smaller is not None) and (mn >= min_smaller):
        raise ValueError(f"Expected smallest value <= {min_smaller}, found: {mn}")

    if (max_larger is not None) and (mx <= max_larger):
        raise ValueError(f"Expected largest value >= {max_larger}, found: {mx}")

    if (max_allowed is not None) and (mx > max_allowed):
        # fix values that are close
        if maybe_fix and np.allclose(mx, max_allowed):
            da = np.fmin(max_allowed, da)
        else:
            raise ValueError(f"Expected no values larger {max_allowed}, found: {mx}")

    if (min_allowed is not None) and (mn < min_allowed):
        # fix values that are close
        if maybe_fix and np.allclose(mn, min_allowed):
            da = np.fmax(min_allowed, da)
        else:
            raise ValueError(f"Expected no values smaller {min_allowed}, found: {mn}")

    return da
