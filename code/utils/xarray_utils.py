import numpy as np
import xarray as xr


def mf_read_netcdfs(
    fNs_in,
    metadata,
    fixes=None,
    fixes_preprocess=None,
):

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
    )

    # get rid of the "days" units, else CDD will have dtype = timedelta
    varn = metadata["varn"]
    units = ds[varn].attrs.get("units", None)
    if units in ["seconds", "days"]:
        ds[varn].attrs.pop("units")

    # ds = xr.decode_cf(ds, use_cftime=True)

    if fixes is not None:
        ds = fixes(ds, metadata)

    check_time_year_simple(ds)

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

def check_time_year_simple(ds, dim="time"):
    # check that all years are present

    if dim in ds.coords:
        year = ds[dim].dt.year
        first_year = year.min().item()
        last_year = year.max().item()
        
        n_years_expected = last_year - first_year + 1
        n_years_actual = len(np.unique(year))
        
        if not n_years_expected == n_years_actual:
            raise ValueError("Missing years!")

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
