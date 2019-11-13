import glob


from ._fixes_common import fixes_common, fixes_hadgem


def cmip5_files(folder_in, metadata):

    exp = metadata["exp"]
    table = metadata["table"]
    varn = metadata["varn"]
    model = metadata["model"]
    ens = metadata["ens"]

    # fix before glob
    if (exp, table, varn, model, ens) == (
        "rcp85",
        "day",
        "tasmax",
        "HadGEM2-ES",
        "r1i1p1",
    ):
        # skip due to mess in files folder
        return None

    # get the files in the directory
    fNs_in = sorted(glob.glob(folder_in))

    # fix after glob

    # some time period exists twice
    if (exp, table, varn, model, ens) == (
        "rcp45",
        "day",
        "tasmax",
        "CMCC-CMS",
        "r1i1p1",
    ):

        offending = "tasmax_day_CMCC-CMS_rcp45_r1i1p1_20060101-20090930.nc"

        fNs_in = [i for i in fNs_in if offending not in i]

    # all after 2100 have a wrong time
    if (exp, table, varn, model, ens) == (
        "rcp45",
        "day",
        "tasmax",
        "GFDL-CM3",
        "r1i1p1",
    ):
        fNs_in = fNs_in[:19]

    return fNs_in


def cmip5_data(ds, metadata, next_path):

    model = metadata["model"]

    # append December of next year, get rid of December of first year
    if "HadGEM2" in model:
        ds = fixes_hadgem(ds, metadata, next_path)

    ds = fixes_common(ds, metadata)

    return ds
