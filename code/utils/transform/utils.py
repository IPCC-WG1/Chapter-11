import xarray as xr


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

            # apply the transformation funcion
            da, attrs = self._trans(da, attrs, **kwargs)

            if len(da) == 0:
                return []

            # back to dataset again
            ds = da.to_dataset(name=self.var)

            # add the attrs again
            ds.attrs = attrs

        return ds

    def _trans(self, da, attrs):
        raise NotImplementedError("Implement _trans in the subclass")

    @property
    def name(self):
        if self._name is None:
            raise NotImplementedError("Please define a name")
        return self._name


def _get_func(object, how):
    """get a function by name"""

    func = getattr(object, how, None)

    if func is None:
        raise KeyError(f"how cannot be '{how}'")

    return func


def alignable(*objects):

    try:
        xr.align(*objects, join="exact", copy=False)
        return True
    except ValueError:
        return False
