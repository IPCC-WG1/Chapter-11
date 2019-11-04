import glob
from ._fixes_common import fixes_common


def cmip6_files(folder_in, metadata):

    exp = metadata["exp"]
    model = metadata["model"]
    varn = metadata["varn"]

    # fix before glob

    fNs_in = glob.glob(folder_in)

    # fixes after glob

    return fNs_in


def cmip6_data(ds, metadata):

    model = metadata["model"]

    if model in ["MCM-UA-1-0"]:
        if "latitude" in ds.dims and "longitude" in ds.dims:
            ds = ds.rename({"latitude": "lat", "longitude": "lon"})

    ds = fixes_common(ds, metadata)

    return ds
