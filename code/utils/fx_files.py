def _get_fx_data(self, varn, meta, table="*", disallow_alternate=False):

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
