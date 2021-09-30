import pandas as pd
import xarray as xr
import yaml

import conf


def save_simulation_info_raw(
    fN,
    da,
    iav=None,
    panel="",
    add_historical=True,
    add_tas=True,
    override=None,
):
    """save raw simulation info from da

    Parameters
    ----------
    fN : str
        File name to save the raw data table to
    da : xr.DataArray
        DataArray containing all plotted model data and their metadata as non-dimension
        coordinates. See ``utils.computation.concat_xarray_with_metadata``
    iav : xr.DataArray, default: None
        DataArray containing all interannual variability data. Not used.
        See ``utils.iav``.
    panel : str, default: ""
        Panel where the data appears, e.g. "a"
    add_historical : bool, default: True
        If the historical simulation should be added for each projection. The
        exp="historical" information is lost for the concatenated simulations.
    add_tas : bool, default: True
        Add varn="tas" for each simulation (also adds historical simulations if
        add_historical is true). Necessary for figures at global warming levels (GWLs).
    override : dict
        Dictionary indicating which keys of the metadata to override for the data
        tables.
    """

    df = _create_simulations_df(
        da,
        iav=iav,
        add_historical=add_historical,
        add_tas=add_tas,
        override=override,
    )

    df["panel"] = panel

    # creates a dict of dicts: the outer dict contains one dict per column
    metas = df.T.to_dict()
    # we need the inner dicts
    metas = list(metas.values())

    with open(fN, "w") as f:
        yaml.safe_dump(metas, f)


def _create_simulations_df(
    da, iav=None, add_historical=True, add_tas=True, override=None
):

    from utils.iav import align_for_iav

    # align da and iav -> assume they are aligned if iav is a DataArray/ Dataset
    if (iav is not None) and (not isinstance(iav, (xr.DataArray, xr.Dataset))):
        aligned = align_for_iav(iav, da)
        da = aligned.da
        iav = aligned.iav_used_models

    df = _create_df(da, override)

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


def _create_df(da, override):
    """turn model info coordinates to a pd.DataFrame"""

    keys = ("model", "ens", "exp", "postprocess", "table", "grid", "varn")

    # get the common keys (intersection)
    common_keys = set(da.coords) & set(keys)

    # read desired keys
    data = {key: da[key].values for key in common_keys}
    df = pd.DataFrame.from_dict(data)

    if override is not None:
        for key, value in override.items():
            df[key] = value

    return df


def _add_historical(df):
    """add historical simulations corresponding to the projections"""

    exp = "historical"

    # all but exp
    keys = ["model", "ens", "postprocess", "table", "grid", "varn"]

    # get the common keys (intersection)
    common_keys = set(keys) & set(df.columns)

    # create a MultiIndex and get unique members
    # we need a historical simulation for each unique combination
    mi = df.set_index(list(common_keys)).index.unique()

    # back to a dataframe
    df_historical = mi.to_frame(index=False)

    # add the historical simulation
    df_historical["exp"] = exp

    df = pd.concat([df, df_historical], axis=0)
    df = df.reset_index(drop=True)

    return df


def _add_for_all(df, **kwargs):
    """duplicate df and update some values"""

    if not kwargs:
        raise ValueError("Must supply at least one column to replace")

    df_to_add = df.copy(deep=True)

    # override values
    for key, value in kwargs.items():
        df_to_add[key] = value

    return df_to_add


def _load_post(meta, conf_cmip):
    """load postprocessed data - to obtain history attributes

    Parameters
    ----------
    meta : dict of metadata
        Metadata of the model data to load.
    conf_cmip : _cmip_conf instance
        conf.cmip5 or conf.cmip6 instance

    Returns
    -------
    ds : xr.Dataset
        loaded Dataset
    meta : dict
        Metadata
    """

    meta_files = meta.copy()
    # remove panel -> not part of the filename
    meta_files.pop("panel", None)

    # might need to set the grid to "*" -> need to load file using load_post_all
    if any("*" in value for value in meta_files.values()):

        try:
            files = conf_cmip.load_post_all(
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
        ds = conf_cmip.load_post(**meta_files)

    return ds, meta


def find_cmip6_info_post(meta):
    """collect cmip6 info for data table for one file

    Parameters
    ----------
    meta : dict of metadata
        Metadata of the model data to load.

    Returns
    -------
    modelinfo : str
        Model info as required for the data tables
    """

    ds, meta = _load_post(meta, conf.cmip6)

    mip_era = conf.cmip6.cmip.upper()
    parent_activity_id = ds.attrs["parent_activity_id"].replace(" ", "")
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


def find_cmip5_info_post(meta):
    """collect cmip5 info for data table for one file

    Parameters
    ----------
    meta : dict of metadata
        Metadata of the model data to load.

    Returns
    -------
    modelinfo : str
        Model info as required for the data tables
    """
    # DATA_REF_SYNTAX;
    # CMIP5.output.MOHC.HadCM3.historical;

    # FREQUENCY;MODELING_REALM;TABLE_ID;ENS_MEMBER;VERSION_NO;VAR_NAME;HANDLE;SUBPANEL
    # mon;atmos;Amon;r8i1p1;None;pr;0bc3c554-f3b0-4d2c-9db8-7979e2bf80ce

    ds, meta = _load_post(meta, conf.cmip5)

    mip_era = conf.cmip5.cmip.upper()
    product = ds.attrs["product"]
    institute_id = ds.attrs["institute_id"]
    model = meta["model"]
    exp = meta["exp"]
    data_ref = ".".join([mip_era, product, institute_id, model, exp])

    frequency = ds.attrs["frequency"]
    modeling_realm = ds.attrs["modeling_realm"]
    table = meta["table"]
    ens = meta["ens"]
    version_no = "none"  # this info is not available in the filename of file
    varn = meta["varn"]
    tracking_id = ds.attrs["tracking_id"]
    panel = meta["panel"]

    one = ";".join(
        [
            data_ref,
            frequency,
            modeling_realm,
            table,
            ens,
            version_no,
            varn,
            tracking_id,
            panel,
        ]
    )

    return one + "\n"


HEADER = dict(
    cmip5=(
        "DATA_REF_SYNTAX;FREQUENCY;MODELING_REALM;TABLE_ID;ENS_MEMBER;VERSION_NO;VAR_NAME;HANDLE;SUBPANEL"
    ),
    cmip6=(
        "DATA_REF_SYNTAX;SUB_EX_ID;ENS_MEMBER;TABLE_ID;VAR_NAME;GRID_LABEL;VERSION_NO;HANDLE;SUBPANEL\n"
    ),
)

FIND_CMIP_INFO_POST = dict(
    cmip6=find_cmip6_info_post,
    cmip5=find_cmip5_info_post,
)


def _find_cmip_info_post_all(df_m, cmip):
    """create data table

    Parameters
    ----------
    df_m : pd.DataFrame
        merged DataFrame containg meta of the files to read
    cmip : {"cmip5", "cmip6"}
        cmip version

    Returns
    -------
    datatable : str
    """

    # function to collect data table info for one model
    find_cmip_info_post = FIND_CMIP_INFO_POST[cmip]

    out = list()
    out.append(HEADER[cmip])

    if cmip == "cmip5":
        df_m = df_m.drop(columns="grid")

    # loop through all models
    for index, row in df_m.iterrows():

        meta = row.to_dict()

        one = find_cmip_info_post(meta)
        out.append(one)

    return out


def _read_yaml(fN):
    """read yaml file with raw data (meta)"""

    with open(fN) as f:
        metas = yaml.safe_load(f)

    # convert to a pandas DataFrame
    return pd.DataFrame.from_records(metas)


def _merge_panels(*dfs):
    """merge dataframes, taking equal models into account

    Parameters
    ----------
    dfs : pd.DataFrame
        DataFrame objects to merge

    Returns
    -------
    df : pd.DataFrame
        merged DataFrame

    Examples
    --------
    >>> import pandas as pd
    >>> d1 = {'model': ["a", "b"], 'panel': ["a", "b"]}
    >>> d2 = {'model': ["a"], 'panel': ["b"]}
    >>> df1 = pd.DataFrame(d1)
    >>> df2 = pd.DataFrame(d2)
    >>> data_tables._merge_panels(df1, df2)
      model panel panel
    0     a     a     b
    1     b     b   NaN

    """

    def get_keys(df):
        return list(set(df.columns) - {"panel"})

    dfs = (df.set_index(get_keys(df)) for df in dfs)

    return pd.concat(dfs, axis=1).reset_index()


def _join_strings(row):
    """join strings - ignoring na"""
    return ",".join(el for el in row if not pd.isna(el))


def cmip_info_post_from_file(fNs, cmip):
    """save cmip5 data table given raw data table files

    Parameters
    ----------
    fNs : str or list of str
        file names of raw data table info (*_md_raw)
    cmip : {"cmip5", "cmip6"}
        cmip version
    """

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

    return _find_cmip_info_post_all(df_m, cmip=cmip)


# ======


def save_cmip5_info_post(fNs, fN_out):
    """save cmip5 data table given raw data table files

    Parameters
    ----------
    fNs : str or list of str
        file names of raw data table info (*_md_raw)
    fN_out : str
        file name to write finalized data table
    """
    data_table = cmip_info_post_from_file(fNs, cmip="cmip5")

    with open(fN_out, "w") as f:
        f.writelines(data_table)


def save_cmip6_info_post(fNs, fN_out):
    """save cmip6 data table given raw data table files

    Parameters
    ----------
    fNs : str or list of str
        file names of raw data table info (*_md_raw)
    fN_out : str
        file name to write finalized data table
    """
    data_table = cmip_info_post_from_file(fNs, cmip="cmip6")

    with open(fN_out, "w") as f:
        f.writelines(data_table)
