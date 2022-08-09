from abc import ABC, abstractmethod

import xarray as xr

from .. import xarray_utils as xru


class TransformWithXarray(ABC):

    _name = None

    def __call__(self, ds, **kwargs):
        """transform dataset according to a transformation function

        Parameters
        ----------
        ds : xr.Dataset
            Dataset to transform
        **kwargs : keyword arguments
            Additional keyword arguments passed to the transformation function.

        """

        # handle non-existing data
        if len(ds) == 0:
            return []
        else:
            # get attrs
            attrs = ds.attrs

            # read single variable
            da = ds[self.var]

            if self.mask is not None:
                da = self.mask_out(da, self.mask)

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

    @staticmethod
    def mask_out(da, mask):

        xru.assert_alignable(mask, da, message="mask has different coordinates!")

        # mask sets True -> NA
        da = da.where(~mask)

        return da

    @abstractmethod
    def _trans(self, da, attrs):
        ...

    @property
    def name(self):
        if self._name is None:
            raise NotImplementedError("Please define a name")
        return self._name
