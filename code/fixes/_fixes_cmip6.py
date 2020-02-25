import glob
from ._fixes_common import fixes_common, _corresponds_to, _remove_matching_fN


def cmip6_files(folder_in):
    def _inner(metadata):

        # fix before glob

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
            metadata,
            table="day",
            varn=["pr"],
            model=["CESM2-WACCM-FV2"],
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
            metadata,
            table="Amon",
            varn="tas",
            model="E3SM-1-1-ECA",
            ens="r1i1p1f1",
        ):
            return None

        # only goes until 2055
        if _corresponds_to(
            metadata,
            table="Amon",
            exp="ssp370"
            varn="tas",
            model="BCC-ESM1",
        ):
            return None

        # only goes until 2055
        if _corresponds_to(
            metadata,
            table="Amon",
            exp="ssp370"
            varn="tas",
            model="MPI-ESM-1-2-HAM",
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

        return fNs_in

    return _inner


def cmip6_data(ds, metadata, next_path):

    if _corresponds_to(metadata, model="MCM-UA-1-0"):
        if "latitude" in ds.dims and "longitude" in ds.dims:
            ds = ds.rename({"latitude": "lat", "longitude": "lon"})

    ds = fixes_common(ds)

    return ds
