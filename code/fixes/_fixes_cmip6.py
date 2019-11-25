import glob
from ._fixes_common import fixes_common, _corresponds_to


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

        # =========================================================================

        # get the files in the directory
        fNs_in = sorted(glob.glob(folder_in))

        # =========================================================================

        # fixes after glob

        return fNs_in

    return _inner


def cmip6_data(ds, metadata, next_path):

    if _corresponds_to(metadata, model="MCM-UA-1-0"):
        if "latitude" in ds.dims and "longitude" in ds.dims:
            ds = ds.rename({"latitude": "lat", "longitude": "lon"})

    ds = fixes_common(ds, metadata)

    return ds
