import pandas as pd
import xarray as xr
import yaml

import conf


def save_simulation_info_raw(
    fN, da, iav=None, panel="", add_historical=True, add_tas=True
):

    df = _create_simulations_df(
        da, iav=iav, add_historical=add_historical, add_tas=add_tas
    )

    df["panel"] = panel

    metas = list(df.T.to_dict().values())

    with open(fN, "w") as f:
        yaml.safe_dump(metas, f)


def _create_simulations_df(da, iav=None, add_historical=True, add_tas=True):

    from utils.iav import align_for_iav

    # align da and iav -> assume they are aligned if iav is a DataArray/ Dataset
    if (iav is not None) and (not isinstance(iav, (xr.DataArray, xr.Dataset))):
        aligned = align_for_iav(iav, da)
        da = aligned.da
        iav = aligned.iav_used_models

    df = _create_df(da)

    if add_historical:
        df = _add_historical(df)

    to_concat = [df]

    if iav is not None:
        df_iav = _create_df(iav)
        to_concat.append(df_iav)

    if add_tas:
        df_tas = _add_for_all(
            df, postprocess="global_mean", table="Amon", varn="tas", grid="*"
        )
        to_concat.append(df_tas)

    df = pd.concat(to_concat, axis=0)
    df = df.reset_index(drop=True)

    return df


def _create_df(da):
    """turn model info coordinates to a pd.DataFrame"""

    keys = ("model", "ens", "exp", "postprocess", "table", "grid", "varn")

    common_keys = set(da.coords) & set(keys)

    dr = {key: da[key].values for key in common_keys}

    df = pd.DataFrame.from_dict(dr)

    return df


def _add_historical(df):
    """add historical simulations corresponding to the projections"""

    exp = "historical"

    # all but exp
    keys = ["model", "ens", "postprocess", "table", "grid", "varn"]

    common_keys = set(keys) & set(df.columns)

    # create a MultiIndex and get unique members
    # we need a historical simulation for each unique combination
    mi = df.set_index(list(common_keys)).index.unique()

    # back to a dataframe
    f = mi.to_frame(index=False)

    # add the historical simulation
    f["exp"] = exp

    df = pd.concat([df, f], axis=0)
    df = df.reset_index(drop=True)

    return df


def _add_for_all(df, **kwargs):
    """duplicate df and update some values"""

    if not kwargs:
        raise ValueError("Must supply at least one column to replace")

    df_to_add = df.copy(deep=True)

    for key, value in kwargs.items():
        df_to_add[key] = value

    return df_to_add


def find_cmip6_info_post(meta):

    # ds = conf.cmip6.load_post(**meta)

    meta_files = meta.copy()
    meta_files.pop("panel", None)

    if any("*" in value for value in meta_files.values()):

        try:
            files = conf.cmip6.load_post_all(
                anomaly=None,
                at_least_until=None,
                year_mean=False,
                **meta_files,
            )
        except ValueError:
            print(meta_files)
            raise

        if len(files) != 1:
            msg = f"Fond {len(files)} simulation for:\n{meta}"
            raise ValueError(msg)

        ds, mta = files[0]

        # grid could be "*" replace it again
        meta["grid"] = mta["grid"]

    else:
        ds = conf.cmip6.load_post(**meta_files)

    mip_era = conf.cmip6.cmip.upper()
    parent_activity_id = ds.attrs["parent_activity_id"]
    institution_id = ds.attrs["institution_id"]
    model = meta["model"]
    exp = meta["exp"]
    data_ref = ".".join([mip_era, parent_activity_id, institution_id, model, exp])

    sub_experiment_id = ds.attrs["sub_experiment_id"]
    ens = meta["ens"]
    table = meta["table"]
    varn = meta["varn"]
    grid = meta["grid"]
    version_no = "none"  # this info is not available in the filename of file
    tracking_id = ds.attrs["tracking_id"]
    panel = meta["panel"]

    one = ";".join(
        [
            data_ref,
            sub_experiment_id,
            ens,
            table,
            varn,
            grid,
            version_no,
            tracking_id,
            panel,
        ]
    )

    return one + "\n"


def _find_cmip6_info_post_all(df_m):

    out = list()

    header = "DATA_REF_SYNTAX;SUB_EX_ID;ENS_MEMBER;TABLE_ID;VAR_NAME;GRID_LABEL;VERSION_NO;HANDLE;SUBPANEL\n"
    out.append(header)

    for index, row in df_m.iterrows():

        meta = row.to_dict()

        one = find_cmip6_info_post(meta)
        out.append(one)

    return out


def _read_yaml(fN):

    with open(fN) as f:
        metas = yaml.safe_load(f)

    # convert to a pandas DataFrame
    return pd.DataFrame.from_records(metas)


def _merge_panels(*dfs):
    def get_keys(df):
        return list(set(df.columns) - {"panel"})

    dfs = (df.set_index(get_keys(df)) for df in dfs)

    return pd.concat(dfs, axis=1).reset_index()


def _join_strings(row):
    return ",".join([el for el in row if not pd.isna(el)])


def save_cmip6_info_post(fNs, fN_out):
    data_table = cmip6_info_post_from_file(fNs)

    with open(fN_out, "w") as f:
        f.writelines(data_table)


def cmip6_info_post_from_file(fNs):

    if isinstance(fNs, str):
        fNs = [fNs]

    dfs = [_read_yaml(fN) for fN in fNs]

    # merge DataFrames
    df_m = _merge_panels(*dfs)

    # get panels
    panel = df_m.pop("panel")

    # maybe merge panels
    if panel.ndim == 1:
        df_m["panel"] = panel
    else:
        df_m["panel"] = panel.apply(_join_strings, axis=1)

    n = len(df_m)
    print(f"Reading {n} files")

    return _find_cmip6_info_post_all(df_m)


# df1 = df.iloc[:3].copy()
# df2 = df.iloc[1:5].copy()
# df3 = df.iloc[2:5].copy()
#
#
#
# df1["panel"] = "a"
# df2["panel"] = "b"
# df3["panel"] = "c"
#
#
#
# def merge_panels(df1, df2):
#
#     keys = list(set(df1.columns) - {"panel"})
#
#     df1 = df1.set_index(keys)
#     df2 = df2.set_index(keys)
#
#     return pd.concat([df1, df2], axis=1).reset_index()
#
#
# dfj = merge_panels(df1, df2)
#
# dfj = merge_panels(dfj, df3)
#
# panel = dfj.pop("panel")
#
#
# def join_strings(row):
#     return ",".join([el for el in row if not pd.isna(el)])
#
#
# dfj["panel"] = panel.apply(join_strings, axis=1)
