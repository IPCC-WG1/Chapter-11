import xarray as xr

from .. import xarray_utils as xru


class _ProcessWithXarray:

    _name = None

    def __call__(self, ds, **kwargs):

        # handle non-existing data
        if len(ds) == 0:
            return []
        else:
            # get attrs
            attrs = ds.attrs

            # read single variable
            da = ds[self.var]

            da = self._maybe_mask_out(da)

            # apply the transformation funcion
            da, attrs = self._trans(da, attrs, **kwargs)

            if len(da) == 0:
                return []

            if isinstance(da, xr.DataArray):
                # back to dataset again
                ds = da.to_dataset(name=self.var)
            else:
                ds = da

            # add the attrs again
            ds.attrs = attrs

        return ds

    def _maybe_mask_out(self, da):

        if self.mask is not None:

            xru.assert_alignable(
                self.mask, da, message="mask has different coordinates!"
            )
            # mask sets True -> NA
            return da.where(~self.mask)

    def _trans(self, da, attrs):
        raise NotImplementedError("Implement _trans in the subclass")

    @property
    def name(self):
        if self._name is None:
            raise NotImplementedError("Please define a name")
        return self._name


def _get_func(obj, how):
    """get a function by name"""

    func = getattr(obj, how, None)

    if func is None:
        raise KeyError(f"how cannot be '{how}'")

    return func
