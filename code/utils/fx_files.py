import numpy as np

from . import xarray_utils as xru


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


def _load_mask_or_weights(self, varn, meta, da=None):

    mask = self.load_fx(varn, meta)

    if mask is not None:

        mask = mask.load().fillna(0.0)

        # we expect the mask to be in 0..100
        # note: will later be converted to 0..1
        if not mask.max() > 1:
            raise ValueError("wrong values in mask/ weights")
        
        if mask.min() < 0:
            # fix values that are almost 0
            if np.allclose(mask.min(), 0):
                mask = np.fmax(0, mask)
            else:
                raise ValueError("wrong values in mask/ weights")
        


        if da is not None:
            mask = xru.maybe_reindex(mask, da)

    return mask
