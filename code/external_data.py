import xarray as xr

import filefinder as ff


def load_exc35(varn="TX"):
    """load TX/ WBGTs data from Clemens Schwingshackl"""

    x = ff.FileFinder(
        path_pattern="../data/Exc35C_TX_WBGTs_BC_Schwingshackl/",
        file_pattern="{thresh}-{varn}_{model}_{exp}_{ens}_{time}_{qualif}.nc",
    )

    files = x.find_files(varn=varn)

    files = ff.cmip.create_ensnumber(files, keys=["exp", "varn", "model"])

    TXmean = list()
    for file, meta in files:
        ds = xr.open_dataset(file, use_cftime=True)

        ds = ds.groupby("time.year").mean("time")

        TXmean.append([ds, meta])

    return TXmean
