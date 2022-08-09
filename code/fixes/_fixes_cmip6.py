import glob

import numpy as np
import xarray as xr

from ._fixes_common import (
    _corresponds_to,
    _remove_matching_fN,
    _remove_non_matching_fN,
    convert_time_to,
    convert_time_to_proleptic_gregorian,
    fixes_common,
)


def cmip6_files(folder_in, meta):
    """fix cmip5 paths and file names

    Parameters
    ----------
    folder_in : str
        Path of the data to load. Must end in "*" for glob.
    meta : dict
        Dictionary containing the metadata of the dataset (variable name, model name
        etc.).

    """

    # fix before glob

    # remove AWI ocean data: has an unstructured grid
    # that I cannot currently handle
    if _corresponds_to(
        meta,
        table=["Oday", "Ofx", "Omon", "SIday", "SImon"],
        model=["AWI-CM-1-1-MR", "AWI-ESM-1-1-LR"],
    ):
        return None

    # tasmax and tasmin are wrong for cesm
    if _corresponds_to(
        meta,
        table="day",
        varn=["tasmax", "tasmin"],
        model=["CESM2", "CESM2-WACCM"],
    ):
        return None

    # the time axis is totally wrong (overlapping)
    if _corresponds_to(meta, table="day", varn=["pr"], model=["CESM2-WACCM-FV2"]):
        return None

    if _corresponds_to(
        meta,
        exp="historical",
        table="day",
        varn="tasmax",
        model="ACCESS-CM2",
        ens="r2i1p1f1",
    ):
        return None

    # non-monotonic time - not sure where...
    if _corresponds_to(
        meta,
        exp="historical",
        table="Amon",
        varn="tas",
        model="EC-Earth3",
        ens="r3i1p1f1",
    ):
        return None

    # not reading
    if _corresponds_to(
        meta,
        exp="ssp119",
        table="Amon",
        varn="tas",
        model="EC-Earth3",
        ens="r102i1p1f1",
    ):
        return None

    # HDF error
    if _corresponds_to(
        meta,
        exp="historical",
        table="day",
        varn="tasmax",
        model="EC-Earth3",
        ens=["r20i1p1f1", "r4i1p1f1", "r3i1p1f1"],
    ):
        return None

    # missing data
    if _corresponds_to(
        meta,
        exp="historical",
        varn=["tas", "tasmax"],
        model="EC-Earth3-Veg",
        ens=["r10i1p1f1"],
    ):
        return None

    # missing data -(will probably be fixed)
    if _corresponds_to(
        meta,
        exp="historical",
        table="Amon",
        varn="tas",
        model="GISS-E2-1-G",
        ens=["r7i1p3f1"],
    ):
        return None

    if _corresponds_to(
        meta,
        table="day",
        varn=["tasmax", "tasmin"],
        model="NorESM2-LM",
        ens="r1i1p1f1",
    ):
        return None

    # has all zero tas in 01.2000 and 01.2007
    if _corresponds_to(
        meta, table="Amon", varn="tas", model="E3SM-1-1-ECA", ens="r1i1p1f1"
    ):
        return None

    # discontinuity between historical and ssp
    if _corresponds_to(
        meta,
        table="day",
        exp=["ssp245", "ssp370"],
        varn="tasmax",
        model="KACE-1-0-G",
    ):
        return None

    # discontinuity between historical and ssp
    if _corresponds_to(
        meta,
        table="Lmon",
        varn="mrsos",
        model="FGOALS-g3",
    ):
        return None

    # time axis not monotonic
    if _corresponds_to(
        meta,
        table="day",
        exp="ssp245",
        varn="tasmax",
        model="KIOST-ESM",
    ):
        return None

    # continents shifted in from 28.02.2018-31.12.2018 (reported)
    if _corresponds_to(
        meta,
        table="day",
        exp="ssp585",
        varn=["tasmax", "tasmin"],
        model="KIOST-ESM",
    ):
        return None

    # all zeros in mrso in Dec 2035 (reported)
    if _corresponds_to(
        meta,
        table="Lmon",
        exp="ssp126",
        varn="mrso",
        model="CIESM",
        ens="r1i1p1f1",
    ):
        return None

    # overlapping files & not sure how to fix them
    if _corresponds_to(
        meta,
        table="Lmon",
        exp="piControl",
        varn=["mrso", "mrsos"],
        model="SAM0-UNICON",
        ens="r1i1p1f1",
    ):
        return None

    # missing years
    if _corresponds_to(
        meta,
        table="Lmon",
        exp="historical",
        varn="mrsos",
        model="CESM2-WACCM-FV2",
        ens="r1i1p1f1",
    ):
        return None

    # negative SM data
    if _corresponds_to(
        meta,
        varn=["mrso", "mrsos"],
        model="IPSL-CM5A2-INCA",
    ):
        return None

    # =========================================================================

    # get the files in the directory
    fNs_in = sorted(glob.glob(folder_in))

    # =========================================================================

    # fixes after glob

    if _corresponds_to(
        meta,
        exp="piControl",
        table="day",
        varn="tasmin",
        model="FIO-ESM-2-0",
        ens="r1i1p1f1",
    ):
        fNs_in = _remove_matching_fN(
            fNs_in,
            "tasmin_day_FIO-ESM-2-0_piControl_r1i1p1f1_gn_03001231-04010109.nc",
            "tasmin_day_FIO-ESM-2-0_piControl_r1i1p1f1_gn_04010110-05010119.nc",
        )

    # duplicate file
    if _corresponds_to(
        meta,
        exp="ssp370",
        table="Amon",
        varn="tas",
        model="CESM2",
        ens="r4i1p1f1",
    ):
        fNs_in = _remove_matching_fN(
            fNs_in, "tas_Amon_CESM2_ssp370_r4i1p1f1_gn_201501-210012.nc"
        )

    # duplicate file
    if _corresponds_to(
        meta,
        exp="ssp585",
        table="Lmon",
        varn="mrso",
        model="NorESM2-LM",
    ):
        fNs_in = _remove_matching_fN(
            fNs_in, "mrso_Lmon_NorESM2-LM_ssp585_r1i1p1f1_gn_201502-202012.nc"
        )

    # remove files that only go to March 2014
    if _corresponds_to(
        meta,
        exp="historical",
        table="day",
        varn=["tasmax", "tasmin"],
        model="KACE-1-0-G",
    ):
        fNs_in = _remove_matching_fN(fNs_in, "_gr_18500101-20140330.nc")

    if _corresponds_to(
        meta,
        exp="historical",
        table="Omon",
        varn="tos",
        model="CIESM",
        ens="r1i1p1f1",
    ):
        fNs_in = _remove_matching_fN(
            fNs_in, "tos_Omon_CIESM_historical_r1i1p1f1_gn_200101-201412.nc"
        )

    if _corresponds_to(
        meta,
        exp="ssp126",
        table="Omon",
        varn="tos",
        model="IITM-ESM",
        ens="r1i1p1f1",
    ):
        fNs_in = _remove_non_matching_fN(
            fNs_in, "tos_Omon_IITM-ESM_ssp126_r1i1p1f1_gn_201501-209912.nc"
        )

    return fNs_in


def cmip6_data(ds, meta):
    """fix loaded cmip5 simulations

    Parameters
    ----------
    ds : xr.Dataset
        Loaded dataset to (potentially) fix.
    meta : dict
        Dictionary containing the metadata of the dataset (variable name, model name
        etc.).

    """

    time_check = True

    if _corresponds_to(meta, model="MCM-UA-1-0"):
        if "latitude" in ds.dims and "longitude" in ds.dims:
            ds = ds.rename({"latitude": "lat", "longitude": "lon"})

    if _corresponds_to(
        meta,
        exp="historical",
        table="day",
        varn="tasmax",
        model="CAMS-CSM1-0",
        ens="r2i1p1f1",
    ):
        ds.load()  # need to load
        ds["tasmax"][dict(time=0)] = np.NaN

    if _corresponds_to(
        meta,
        exp="historical",
        table=["Oday", "Ofx", "Omon", "SIday", "SImon"],
        model="EC-Earth3-Veg",
        ens=["r1i1p1f1", "r5i1p1f1"],
    ):
        if "time" in ds.coords:
            ds = convert_time_to_proleptic_gregorian(ds)

    if _corresponds_to(
        meta,
        varn="siconc",
        table="SImon",
        model="NESM3",
        ens="r1i1p1f1",
    ):
        if "time" in ds.coords:
            ds = convert_time_to(ds, "noleap")

    # not reading without load...
    if _corresponds_to(
        meta,
        table="day",
        varn=["pr", "tasmax"],
        model="EC-Earth3-Veg-LR",
        ens="r1i1p1f1",
    ):
        ds.load()

    # overwrite ice with NaN
    if _corresponds_to(
        meta,
        varn=["mrso", "mrsos"],
        model=[
            "EC-Earth3",
            "EC-Earth3-AerChem",
            "EC-Earth3-Veg",
            "EC-Earth3-Veg-LR",
            "EC-Earth3-CC",
        ],
    ):
        ds = ds.load()
        # mask gridpoints with constant values
        mask = (ds.isel(time=0) == ds).all("time")
        ds = ds.where(~mask)

    # overwrite ice with NaN
    if _corresponds_to(
        meta,
        varn=["mrso", "mrsos"],
        model=["MIROC6"],
    ):
        ds = ds.load()
        # mask gridpoints with constant values
        mask = (ds.isel(time=0) == ds).all("time")
        ds = ds.where(~mask)

    # overwrite ice with NaN
    if _corresponds_to(
        meta,
        varn=["mrso", "mrsos"],
        model=["BCC-CSM2-MR"],
    ):
        ds = ds.load()
        # mask gridpoints with constant values
        mask = (ds.isel(time=0) == ds).all("time")
        ds = ds.where(~mask)

    # overwrite 0 with NaN
    # there is a small danger SM is really 0 at a gridpoint
    if _corresponds_to(
        meta,
        varn=["mrso", "mrsos"],
    ):
        ds = ds.where(ds != 0)

    if _corresponds_to(
        meta,
        table="fx",
        varn="sftlf",
        model="E3SM-1-0",
        ens="r1i1p1f1",
        exp="piControl",
    ):
        # land_area_fraction is given as 0..1
        ds = ds * 100

        if ds.max().sftlf > 100:
            mx = ds.max().compute()
            raise ValueError(f"They replaced the land_area_fraction file... {mx}")

    if _corresponds_to(
        meta,
        table="fx",
        varn="sftlf",
        model="FGOALS-f3-L",
        ens="r1i1p1f1",
        exp="historical",
    ):
        # land_area_fraction is given as 0..1
        ds = ds * 100

        if ds.max().sftlf > 100:
            mx = ds.max().compute()
            raise ValueError(f"They replaced the land_area_fraction file... {mx}")

    if _corresponds_to(
        meta,
        table="Lmon",
        varn="mrsos",
        model="CESM2-WACCM-FV2",
        ens="r1i1p1f1",
        exp="piControl",
    ):
        time_check = False

    # 5 missing days
    if _corresponds_to(
        meta,
        table="day",
        varn="pr",
        model="CESM2-WACCM",
        ens="r1i1p1f1",
        exp="piControl",
    ):
        time = ds.time
        # get the full time vector
        time = xr.cftime_range(time[0].item(), time[-1].item())
        ds = ds.reindex(time=time)

    # misses 01.01.1950 -> I think this is ok
    if _corresponds_to(
        meta,
        table="day",
        varn=["mrso", "tasmax", "tasmin"],
        model="SAM0-UNICON",
        ens="r1i1p1f1",
        exp="historical",
    ):
        time_check = False

    # cdd cannot handle a missing day
    if _corresponds_to(
        meta,
        table="day",
        varn="pr",
        model="SAM0-UNICON",
        ens="r1i1p1f1",
        exp="historical",
    ):
        time = ds.time
        # get the full time vector
        time = xr.cftime_range(time[0].item(), time[-1].item())
        ds = ds.reindex(time=time)

    # mrso is a factor 100 smaller than any other model (reported 19.01.2021)
    if _corresponds_to(
        meta,
        table="Lmon",
        varn="mrso",
        model="CIESM",
    ):

        if ds.mrso.max() > 100:
            mx = ds.max().compute()
            raise ValueError(f"File corrected... {mx}")

        # land_area_fraction is given as 0..1
        ds["mrso"] = ds.mrso * 100

    # mrsos is a factor 100 smaller than any other model (reported 19.01.2021)
    if _corresponds_to(
        meta,
        table="Lmon",
        varn="mrsos",
        model="FGOALS-f3-L",
    ):
        if ds.mrsos.max() > 1:
            mx = ds.max().compute()
            raise ValueError(f"File corrected... {mx}")

        # land_area_fraction is given as 0..1
        ds["mrsos"] = ds.mrsos * 100

    # pr too small by a factor 1000; reported & waiting for confirmation
    if _corresponds_to(
        meta,
        varn="pr",
        model="CIESM",
    ):
        if ds.pr.max() > 1e-3:
            mx = ds.max().compute()
            raise ValueError(f"File corrected... {mx}")

        ds["pr"] = ds.pr * 1000

    # wrong encoding for the ocean & values at the North Pole
    if _corresponds_to(
        meta,
        exp=["historical", "piControl"],
        table="Lmon",
        varn="mrsos",
        model="CAS-ESM2-0",
        ens="r1i1p1f1",
    ):
        da = ds["mrsos"]
        da = (da).where(da != -9999).where(da.lat < 85)
        ds["mrsos"] = da

    # data that should not be below 0; SM precip
    if _corresponds_to(
        meta,
        varn=["pr", "mrso", "mrsos"],
    ):

        varn = meta["varn"]
        min_allowed = 0.0
        mn = ds[varn].min().compute().item()

        if mn < min_allowed:
            # fix values that are close
            if np.allclose(mn, min_allowed):
                ds[varn] = np.fmax(min_allowed, ds[varn])
            else:
                raise ValueError(
                    f"Expected no values smaller {min_allowed}, found: {mn}"
                )

    return ds, time_check


def cmip6_preprocess(fNs_in, meta):
    """fix cmip6 simulations in the preprocess step

    Parameters
    ----------
    fNs_in : xr.Dataset
        Loaded dataset to (potentially) fix.
    meta : dict
        Dictionary containing the metadata of the dataset (variable name, model name
        etc.).

    """

    reindex_like = False

    # there are small differences in the lat/ lon coords
    if _corresponds_to(
        meta,
        exp="historical",
        table="Amon",
        model="NorCPM1",
    ):

        reindex_like = True
        target = xr.open_dataset(fNs_in[0], drop_variables=["tas", "time"])[
            ["lat", "lon"]
        ]

    def _inner(ds):

        if reindex_like:
            ds = ds.reindex_like(target, method="nearest")

        ds = fixes_common(ds)

        return ds

    return _inner
