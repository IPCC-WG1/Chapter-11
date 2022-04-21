import regionmask

from .. import xarray_utils as xru

# TODO: include this into cmip_conf. Unfortunatly I included this in the Processor
# instead of conf_cmip - I am not rewriting this now to avoid errors


class MasksMixin:
    """helper class to load CMIP boolean masks to mask out grid cells"""

    def get_masks(self, masks, meta, da=None):
        """get one or several combined masks to mask out grid cells

        masks are combined by logical ``or``

        Parameters
        ----------
        masks : str, list of str, or None
            Name of the mask(s) to load, if None returns None.
        meta : meta-dict
            meta-dict of the simulation to load the mask for
        da : DataArray
            Model data, used to reindex the mask to if the grids don't exactly match.
        """

        # empty da -> no need to load anything
        if isinstance(da, list):
            return

        if isinstance(masks, (str, regionmask.Regions)):
            masks = [masks]
        elif masks is None:
            masks = []

        # initialize as no mask
        mask_out = False

        for name in masks:
            if isinstance(name, str):
                other = getattr(self, f"get_{name}_mask")(meta, da)
            elif isinstance(name, regionmask.Regions):
                other = self.get_regions_mask(name, meta, da)

            if other is not None:
                mask_out = mask_out | other

        if mask_out is False:
            mask_out = None

        return mask_out

    def get_land_mask(self, meta, da=None):
        """
        load land mask ('sftlf'), uses 'natural_earth.land_110' if none is found

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        mask : xr.DataArray
        """

        mask = self.conf_cmip.load_mask("sftlf", meta, da)

        if mask is None:
            mask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            mask = mask.squeeze(drop=True)

        return mask

    def get_ocean_mask(self, meta, da=None):
        """load ocean mask (inverse of the land mask)

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        mask : xr.DataArray
        """

        return ~self.get_land_mask(meta, da=da)

    def get_landice_mask(self, meta, da=None):
        """load landice mask ('sftgif')

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        mask : xr.DataArray
        """

        return self.conf_cmip.load_mask("sftgif", meta, da)

    def get_antarctica_mask(self, meta, da):
        """load mask of Antarctica (lat < -60)

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        mask : xr.DataArray
        """

        if da is None:
            raise TypeError("'da' required to mask out Antarctica")

        return da.lat < -60

    def get_regions_mask(self, regions, meta, da):
        """load mask of arbitrary ``regionmask.Regions``

        Parameters
        ----------
        regions : regionmask.Regions
            Regions to mask out.
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        mask : xr.DataArray
        """

        if da is None:
            raise TypeError("'da' required to mask out 'regions'")

        if not isinstance(regions, regionmask.Regions):
            raise TypeError("'regions' must be a 'regionmask.Regions' instance")

        mask = regions.mask_3D(da)
        return mask.any("region")


class WeightsMixin:
    """helper class to load CMIP area weights required for weighed averages"""

    def get_area_weights(self, lat_weights_name, weights, meta, ds):
        """combine lat_weights with one or none of the other available weighs

        Parameters
        ----------
        lat_weights_name : str
            Name of the latitude weights variable (e.g. "areacella", "areacello").
        weights : str
            Name of the weights to load. Must be one of the methods available here. E.g.
            pass ``"land"`` to call ``get_land_weights``.
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        ds : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        weights : xr.DataArray
        """

        area_weights = self.get_lat_weights(lat_weights_name, meta, ds)

        # lat weights are required
        if area_weights is None:
            return []

        weights = self.get_weights(weights, meta, ds)
        if weights is not None:
            area_weights = area_weights * weights

        return area_weights

    def get_weights(self, weights, meta, da=None):
        """get weights by name

        Parameters
        ----------
        weights : str
            Name of the weights to load. Must be one of the methods available here. E.g.
            pass ``"land"`` to call ``get_land_weights``.
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        weights : xr.DataArray
        """

        if weights is None:
            return None

        if not isinstance(weights, str):
            raise ValueError(f"'weights' must be a str. Found: {weights}")

        return getattr(self, f"get_{weights}_weights")(meta, da)

    def get_lat_weights(self, lat_weights_name, meta, ds):
        """get lat weights from fx if possible, else as cos(lat) unless ds is 2D

        Parameters
        ----------
        lat_weights_name : str
            Name of the latitude weights variable (e.g. "areacella", "areacello").
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        ds : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        weights : xr.DataArray
        """

        # return if ds is empty
        if not len(ds):
            return

        weights = None
        if lat_weights_name is not None:
            weights = self.conf_cmip.load_fx(lat_weights_name, meta)

        if weights is not None:
            weights = xru.check_range(weights, min_allowed=0)
            weights = xru.maybe_reindex(weights, ds)

        # "latitude" means there are 2D coords, should not use cos weights for non
        # regular grids
        if weights is None and "latitude" not in ds.coords:
            weights = xru.cos_wgt(ds)

        return weights

    def get_land_weights(self, meta, da=None):
        """
        load land weights ('sftlf'), uses 'natural_earth.land_110' if none is found

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        weights : xr.DataArray
        """

        mask = self.conf_cmip.load_weights("sftlf", meta, da)

        # has weights of 0 and 1
        if mask is None:
            mask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            mask = mask.squeeze(drop=True)

        return mask

    def get_ocean_weights(self, meta, da=None):
        """
        load ocean weights - the inverse of land weights

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the lat_weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        weights : xr.DataArray
        """

        return 1 - self.get_land_weights(meta, da=da)

    def get_landice_weights(self, meta, da=None):
        """load landice weights ('sftgif')

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        weights : xr.DataArray
        """

        return self.conf_cmip.load_weights("sftgif", meta, da)

    def get_land_no_ice_weights(self, meta, da=None):
        """load weights for land excluding landice

        Parameters
        ----------
        meta : dict of metadata
            Metadata of the model to load the fx files for.
        da : xr.DataArray or xr.Dataset
            An array of the model to load the weights for. Used to ensure alignment
            of the fx data and the data.

        Returns
        -------
        weights : xr.DataArray
        """

        land = self.get_land_weights(meta, da=da)
        landice = self.get_landice_weights(meta, da=da)

        # I am not entirely sure here. This could also be (land - landice). However,
        # this results in *negative* land fraction for some models.
        if landice is not None:
            land = (land) - ((land) * (landice))

        return land
