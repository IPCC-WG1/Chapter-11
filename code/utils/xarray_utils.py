import warnings

import numpy as np
import xarray as xr


def mf_read_netcdfs(
    fNs_in,
    metadata,
    fixes=None,
    fixes_preprocess=None,
    check_time=True,
):

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
    ).persist()

    # get rid of the "days" units, else CDD will have dtype = timedelta
    varn = metadata["varn"]
    units = ds[varn].attrs.get("units", None)
    if units in ["seconds", "days"]:
        ds[varn].attrs.pop("units")

    # ds = xr.decode_cf(ds, use_cftime=True)

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


# =============================================================================


def cos_wgt(ds, lat="lat"):
    """cosine-weighted latitude"""
    return np.cos(np.deg2rad(ds[lat]))


# =============================================================================


def all_years(ds, dim="time", errors="raise"):
    # check that all years are present

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

    if dim in ds.coords:

        time = ds[dim]
        dt = time.diff(dim)
        delta_days = dt.dt.days
        dd0 = delta_days[0]

        # daily data
        if dd0 == 1:

            if (delta_days != 1).any():
                raise ValueError("Missing days in daily data")

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


# =============================================================================


def alignable(*objects):

    try:
        xr.align(*objects, join="exact", copy=False)
        return True
    except ValueError:
        return False


def assert_alignable(*objects, message=""):

    if not alignable(*objects):
        raise ValueError(message)


def maybe_reindex(da, target):

    if alignable(da, target):
        return da

    if np.allclose(da.lat, target.lat) and np.allclose(da.lon, target.lon):
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
        if np.allclose(mx, max_allowed):
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
