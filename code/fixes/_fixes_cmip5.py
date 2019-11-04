import glob


from ._fixes_common import fixes_common


def cmip5_files(folder_in, metadata):

    exp = metadata["exp"]
    model = metadata["model"]
    varn = metadata["varn"]

    # fix before glob

    fNs_in = glob.glob(folder_in)

    # fixes after glob

    return fNs_in


def cmip5_data(ds, metadata):

    model = metadata["model"]

    ds = fixes_common(ds, metadata)

    return ds
