from . import xarray_utils as xru


def _get_fx_data(self, varn, meta, table="*", disallow_alternate=False):
    """load cmip fx data with fallbacks

    Parameters
    ----------
    varn : str
        Variable name to load.
    meta : dict of metadata
        Metadata of the model to load the fx files for. Note incompatible metadata
        (e.g. "varn") will be automatically removed from meta.
    table : str, default "*"
        Which 'table' to look for the fx files. Note that cmip6 has the tables 'fx'
        and 'Ofx', therefore we use a wildcard per default.
    disallow_alternate : bool, default: False
        If we are allowed to look for fx files of the model for different 'exp' or
        'ens' than the ones specified in 'meta'.

    Returns
    -------
    fx : xr.DataArray or None
        Returns the fx DataArray if found, else returns None.
    """

    # only retain necessary keys in meta (e.g. discard ensnumber)
    keys = self.files_fx.keys - set(["table", "varn"])
    meta = {key: meta[key] for key in keys}

    # try to get an exact match
    fC = self.files_fx.find_files(varn=varn, table=table, _allow_empty=True, **meta)

    if len(fC) == 1:
        return fC[0]
    elif len(fC) > 1:
        msg = f"found more than one fx file for {varn}, {table}, {meta}"
        raise ValueError(msg)
    elif disallow_alternate:
        msg = f"Found no 'fx' file for {varn}, {table}, {meta}"
        raise ValueError(msg)

    # try without exp
    exp = meta.pop("exp", None)
    fC = self.files_fx.find_files(varn=varn, table=table, _allow_empty=True, **meta)

    if len(fC):
        return fC[0]

    # try without ens
    meta.pop("ens", None)
    meta["exp"] = exp
    fC = self.files_fx.find_files(varn=varn, table=table, _allow_empty=True, **meta)

    if len(fC):
        return fC[0]

    # try without both
    meta.pop("exp", None)
    fC = self.files_fx.find_files(varn=varn, table=table, _allow_empty=True, **meta)

    if len(fC):
        return fC[0]

    return None, None


def _load_mask_or_weights(self, varn, meta, da=None):
    """load fx file with nas filled and aligned to da"""

    mask = self.load_fx(varn, meta)

    if mask is not None:

        mask = mask.load().fillna(0.0)

        # get rid of the 'type' coordinate, it can cause problems for cdo
        mask = mask.drop_vars("type", errors="ignore")

        # check range of the weights
        mask = xru.check_range(
            mask,
            min_allowed=0.0,
            max_allowed=100.0,
            max_larger=1.1,  # make sure it's percentage (0..100) and not fraction (0..1)
        )

        if da is not None:
            mask = xru.maybe_reindex(mask, da)

    return mask
