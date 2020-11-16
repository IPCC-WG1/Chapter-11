import pandas as pd
import parse

from ._filefinder import FileContainer

# select preferred grid; order indicates priority
VALID_GRIDS = ("gn", "gr", "gr1", "gm")


def parse_ens(filelist):

    ens = filelist.df["ens"]

    # for cmip6
    if "f" in ens.iloc[0]:
        parser = parse.compile("r{r}i{i}p{p}f{f}")
    # for cmip5
    else:
        parser = parse.compile("r{r}i{i}p{p}")

    out = list()
    for i, one_ens in zip(ens.index, ens):
        parsed = parser.parse(one_ens)
        out.append(list(parsed.named.values()))

    keys = list(parsed.named.keys())

    df = pd.DataFrame(out, columns=keys)

    for key in df.columns:
        filelist.df[key] = df[key].values

    # filelist.df = filelist.df.assign(**df)
    return filelist


def create_ensnumber(filelist, keys=["exp", "table", "varn", "model"]):

    df = filelist.df
    combined = filelist.combine_by_key(keys)

    df["ensnumber"] = -1

    for comb in combined.unique():

        sel = combined == comb
        numbers = list(range(sel.sum()))
        df.loc[sel, "ensnumber"] = numbers

    filelist.df = df
    return filelist


def _make_unique_grids(filelist, mi):

    out = list()

    # loop simulations
    for idx in mi.unique():

        one_ = filelist.df.iloc[mi.get_locs(idx)]

        # there is only one grid for this simulation
        if len(one_) == 1:
            out.append(one_)
            continue

        # find the preferred grid
        for grid in VALID_GRIDS:
            if grid in one_.grid.values:

                out.append(one_[one_.grid == grid])
                break

    df = pd.concat(out, axis=0)
    df = df.reset_index(drop=True)

    return df


def ensure_unique_grid(filelist):
    """ensure there is only one grid per simulation
    """

    # each simulation must be unique in the combination of these keys
    keys = ["exp", "table", "varn", "model", "ens"]

    # create a MultiIndex -> can directly index over multiple keys
    mi = pd.MultiIndex.from_frame(filelist.df[keys])

    # there are models with more than one grid
    if mi.has_duplicates:

        df = _make_unique_grids(filelist, mi)

        # double check
        mi = pd.MultiIndex.from_frame(df[keys])

        if mi.has_duplicates:
            raise ValueError("Something went wrong")

        filelist = FileContainer(df)

    return filelist
