import glob

import numpy as np

from ._fixes_common import (
    _corresponds_to,
    _remove_matching_fN,
    _remove_non_matching_fN,
    convert_time_to,
    convert_time_to_proleptic_gregorian,
    fixes_common,
)


def cmip6_files(folder_in):
    def _inner(metadata):

        # fix before glob

        # remove AWI ocean data: has an unstructured grid
        # that I cannot currently handle
        if _corresponds_to(
            metadata,
            table=["Oday", "Ofx", "Omon", "SIday", "SImon"],
            model=["AWI-CM-1-1-MR", "AWI-ESM-1-1-LR"],
        ):
            return None

        # tasmax and tasmin are wrong for cesm
        if _corresponds_to(
            metadata,
            table="day",
            varn=["tasmax", "tasmin"],
            model=["CESM2", "CESM2-WACCM"],
        ):
            return None

        # the time axis is totally wrong (overlapping)
        if _corresponds_to(
            metadata, table="day", varn=["pr"], model=["CESM2-WACCM-FV2"]
        ):
            return None

        if _corresponds_to(
            metadata,
            exp="historical",
            table="day",
            varn="tasmax",
            model="ACCESS-CM2",
            ens="r2i1p1f1",
        ):
            return None

        # non-monotonic time - not sure where...
        if _corresponds_to(
            metadata,
            exp="historical",
            table="Amon",
            varn="tas",
            model="EC-Earth3",
            ens="r3i1p1f1",
        ):
            return None

        # not reading
        if _corresponds_to(
            metadata,
            exp="ssp119",
            table="Amon",
            varn="tas",
            model="EC-Earth3",
            ens="r102i1p1f1",
        ):
            return None

        # HDF error
        if _corresponds_to(
            metadata,
            exp="historical",
            table="day",
            varn="tasmax",
            model="EC-Earth3",
            ens=["r20i1p1f1", "r4i1p1f1"],
        ):
            return None

        # missing data
        if _corresponds_to(
            metadata,
            exp="historical",
            table="Amon",
            varn="tas",
            model="EC-Earth3-Veg",
            ens=["r10i1p1f1"],
        ):
            return None

        # missing data -(will probably be fixed)
        if _corresponds_to(
            metadata,
            exp="historical",
            table="Amon",
            varn="tas",
            model="GISS-E2-1-G",
            ens=["r7i1p3f1"],
        ):
            return None

        if _corresponds_to(
            metadata,
            table="day",
            varn=["tasmax", "tasmin"],
            model="NorESM2-LM",
            ens="r1i1p1f1",
        ):
            return None

        # has all zero tas in 01.2000 and 01.2007
        if _corresponds_to(
            metadata, table="Amon", varn="tas", model="E3SM-1-1-ECA", ens="r1i1p1f1"
        ):
            return None

        # only goes until 2055
        if _corresponds_to(
            metadata, table="Amon", exp="ssp370", varn="tas", model="BCC-ESM1"
        ):
            return None

        # only goes until 2055
        if _corresponds_to(
            metadata, table="Amon", exp="ssp370", varn="tas", model="MPI-ESM-1-2-HAM"
        ):
            return None

        # the global mean temperature decreases ~8K after 2090
        # if _corresponds_to(
        #     metadata, table="Amon", exp="ssp585", varn="tas", model="CIESM",
        # ):
        #     return None

        # discontinuity between historical and ssp
        if _corresponds_to(
            metadata,
            table="day",
            exp=["ssp245", "ssp370"],
            varn="tasmax",
            model="KACE-1-0-G",
        ):
            return None

        # discontinuity between historical and ssp
        if _corresponds_to(
            metadata,
            table="Lmon",
            varn="mrsos",
            model="FGOALS-g3",
        ):
            return None

        # time axis not monotonic
        if _corresponds_to(
            metadata,
            table="day",
            exp="ssp245",
            varn="tasmax",
            model="KIOST-ESM",
        ):
            return None

        # all zeros in mrso in Dec 2035 (reported)
        if _corresponds_to(
            metadata,
            table="Lmon",
            exp="ssp126",
            varn="mrso",
            model="CIESM",
            ens="r1i1p1f1",
        ):
            return None

        # =========================================================================

        # get the files in the directory
        fNs_in = sorted(glob.glob(folder_in))

        # =========================================================================

        # fixes after glob

        if _corresponds_to(
            metadata,
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
            metadata,
            exp="ssp370",
            table="Amon",
            varn="tas",
            model="CESM2",
            ens="r4i1p1f1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in, "tas_Amon_CESM2_ssp370_r4i1p1f1_gn_201501-210012.nc"
            )

        # remove files that only go to March 2014
        if _corresponds_to(
            metadata,
            exp="historical",
            table="day",
            varn=["tasmax", "tasmin"],
            model="KACE-1-0-G",
        ):
            fNs_in = _remove_matching_fN(fNs_in, "_gr_18500101-20140330.nc")

        if _corresponds_to(
            metadata,
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
            metadata,
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

    return _inner


def cmip6_data(ds, metadata):

    if _corresponds_to(metadata, model="MCM-UA-1-0"):
        if "latitude" in ds.dims and "longitude" in ds.dims:
            ds = ds.rename({"latitude": "lat", "longitude": "lon"})

    ds = fixes_common(ds)

    if _corresponds_to(
        metadata,
        exp="historical",
        table="day",
        varn="tasmax",
        model="CAMS-CSM1-0",
        ens="r2i1p1f1",
    ):
        ds.load()  # need to load
        ds["tasmax"][dict(time=0)] = np.NaN

    if _corresponds_to(
        metadata,
        exp="historical",
        table=["Oday", "Ofx", "Omon", "SIday", "SImon"],
        model="EC-Earth3-Veg",
        ens=["r1i1p1f1", "r5i1p1f1"],
    ):
        if "time" in ds.coords:
            ds = convert_time_to_proleptic_gregorian(ds)

    if _corresponds_to(
        metadata,
        varn="siconc",
        table="SImon",
        model="NESM3",
        ens="r1i1p1f1",
    ):
        if "time" in ds.coords:
            ds = convert_time_to(ds, "noleap")

    # not reading without load...
    if _corresponds_to(
        metadata,
        table="day",
        varn=["pr", "tasmax"],
        model="EC-Earth3-Veg-LR",
        ens="r1i1p1f1",
    ):
        ds.load()

    # overwrite ice with NaN
    if _corresponds_to(
        metadata,
        varn=["mrso", "mrsos"],
        model=["EC-Earth3", "EC-Earth3-AerChem", "EC-Earth3-Veg", "EC-Earth3-Veg-LR"],
    ):
        ds = ds.load()
        # mask gridpoints with constant values
        mask = (ds.isel(time=0) == ds).all("time")
        ds = ds.where(~mask)

    # overwrite 0 with NaN
    # there is a small danger SM is really 0 at a gridpoint
    if _corresponds_to(
        metadata,
        varn=["mrso", "mrsos"],
    ):
        ds = ds.where(ds != 0)

    return ds
