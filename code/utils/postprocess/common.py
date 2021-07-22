import time
import traceback

import regionmask

from .. import xarray_utils as xru
from ..file_utils import _any_file_does_not_exist


class Processor:
    def __init__(self, conf_cmip):

        self.conf_cmip = conf_cmip

        self._postprocess_name = None
        self._all_files = None

        self._files_kwargs = None

    @property
    def postprocess_name(self):
        if self._postprocess_name is None:
            raise AttributeError("postprocess_name is not set!")
        return self._postprocess_name

    @postprocess_name.setter
    def postprocess_name(self, value):
        self._postprocess_name = value

    @property
    def all_files(self):
        """all files to postprocess, inizialize with find_all_files_*"""

        return self._all_files

    @all_files.setter
    def all_files(self, value):

        if self._all_files is not None:
            raise ValueError("files already set!")

        print(f"Found {len(value)} simulations")
        self._all_files = value

    def set_files_kwargs(self, **kwargs):
        self._files_kwargs = kwargs

    def _find_new_simulations(self):

        files = self.all_files

        files_to_process = list()

        for file, meta in files:

            fN_out = self.fN_out(**meta)

            if _any_file_does_not_exist(fN_out):
                files_to_process.append([file, meta])

        print(f"{len(files_to_process)} unprocessed simulations", end="\n\n")
        self.files_to_process = files_to_process

    def fN_out(self, **meta):

        # make sure postprocess does not get deleted
        meta = meta.copy()

        meta.pop("postprocess", None)
        return self.conf_cmip.files_post.create_full_name(
            postprocess=self.postprocess_name, **meta
        )

    def _yield_transform(self, **kwargs):

        print("")
        print(f"Processing {str(self)}")
        print(f"- files_kwargs: {self._files_kwargs}")
        print("")

        self.find_all_files()
        self._find_new_simulations()

        self.transform_kwargs = kwargs
        n_to_process = len(self.files_to_process)

        if self.files_to_process:
            self.conf_cmip._create_folder_for_output(
                self.files_to_process, self.postprocess_name
            )

        for i, (file, meta) in enumerate(self.files_to_process):
            now = time.strftime("%Y.%m.%d %H:%M:%S", time.localtime())
            fN_out = self.fN_out(**meta)
            print(f"processing {i + 1} of {n_to_process} ({now})")
            print(f"- {meta}")
            print(f"- {file}")
            print(f"- {fN_out}")

            yield file, meta

    def transform(self, **kwargs):

        for file, meta in self._yield_transform(**kwargs):

            try:
                ds = self._transform(**meta)
                fN_out = self.fN_out(**meta)
                self.save(ds, fN_out)
            except Exception as err:
                raise err
                # traceback.print_tb(err.__traceback__)

    def save(self, ds, fN_out):

        # make folder

        if ds is None:
            raise ValueError("Forgot to return ds?")

        if len(ds) != 0:
            ds.to_netcdf(fN_out, format="NETCDF4_CLASSIC")

    def get_area_weights(self, lat_weights, weights, meta, ds):
        """get area weights: lat weights and weights for land, landice etc."""

        area_weights = self.get_lat_weights(lat_weights, meta, ds)
        # lat weights are required
        if area_weights is None:
            return []

        weights = self.get_weights(weights, meta, ds)
        if weights is not None:
            area_weights = area_weights * weights

        return area_weights

    def get_lat_weights(self, lat_weights, meta, ds):

        # return if ds is empty
        if not len(ds):
            return

        weights = None
        if lat_weights is not None:
            weights = self.conf_cmip.load_fx(lat_weights, meta)

        if weights is not None:
            weights = xru.check_range(weights, min_allowed=0)
            weights = xru.maybe_reindex(weights, ds)

        # "latitude" means there are 2D coords
        if weights is None and "latitude" not in ds.coords:
            weights = xru.cos_wgt(ds)

        return weights

    def get_land_mask(self, meta, da=None):
        """
        load land mask ('sftlf'), uses 'natural_earth.land_110' if none is found
        """

        mask = self.conf_cmip.load_mask("sftlf", meta, da)

        if mask is None:
            mask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            mask = mask.squeeze(drop=True)

        return mask

    def get_ocean_mask(self, meta, da=None):
        """load ocean mask (inverse of the land mask)"""

        return ~self.get_land_mask(meta, da=da)

    def get_landice_mask(self, meta, da=None):
        """load landice mask ('sftgif')"""

        return self.conf_cmip.load_mask("sftgif", meta, da)

    def get_antarctica_mask(self, meta, da):

        if da is None:
            raise ValueError("'da' required to mask out Antarctica")

        return da.lat < -60

    def get_regions_mask(self, regions, meta, da):

        if da is None:
            raise ValueError("'da' required to mask out 'regions'")

        if not isinstance(regions, regionmask.Regions):
            raise ValueError("'regions' must be a 'regionmask.Regions' instance")

        mask = regions.mask_3D(da)
        return mask.any("region")

    def get_masks(self, masks, meta, da=None):
        """get one or several combined masks to mask out

        masks are combined by logical or

        Parameters
        ----------
        masks : str, or list of strings, optional
            Name of the mask(s) to load, if None returns None.
        meta : meta-dict
            meta-dict of the simulation to load the mask for
        da : DataArray
            Model data, used to reindex the mask to if the grids don't exactly match.
        """

        # empty da -> no need to load anything
        if issubclass(type(da), list):
            return

        if isinstance(masks, (str, regionmask.Regions)):
            masks = [masks]
        elif masks is None:
            masks = []

        # initialize as no mask
        mask = False

        for m in masks:
            if isinstance(m, str):
                other = getattr(self, f"get_{m}_mask")(meta, da)
            elif isinstance(m, regionmask.Regions):
                other = self.get_regions_mask(m, meta, da)

            if other is not None:
                mask = mask | other

        if mask is False:
            mask = None

        return mask

    def get_land_weights(self, meta, da=None):

        mask = self.conf_cmip.load_weights("sftlf", meta, da)

        if mask is None:
            mask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            mask = mask.squeeze(drop=True)

        return mask

    def get_ocean_weights(self, meta, da=None):

        return 1 - self.get_land_weights(meta, da=da)

    def get_landice_weights(self, meta, da=None):
        return self.conf_cmip.load_weights("sftgif", meta, da)

    def get_land_no_ice_weights(self, meta, da=None):

        land = self.get_land_weights(meta, da=da)
        landice = self.get_landice_weights(meta, da=da)

        # I am not entirely sure here. This could also be (land - landice). However,
        # this results in *negative* land fraction for some models.
        if landice is not None:
            land = (land) - ((land) * (landice))

        return land

    def get_weights(self, weights, meta, da=None):

        if weights is None:
            return None

        if not isinstance(weights, str):
            raise ValueError(f"'weights' must be a str. Found: {weights}")

        return getattr(self, f"get_{weights}_weights")(meta, da)

    def _transform(self, **meta):
        raise NotImplementedError("")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        """provide a nice str repr of our Weighted object"""

        klass = self.__class__.__name__
        ppn = self._postprocess_name
        ppn = "" if ppn is None else f" ({ppn})"
        cmip = self.conf_cmip.cmip.upper()

        return f"{cmip}: <{klass}>{ppn}"


class ProcessorFromOrig(Processor):
    def find_all_files(self):

        self.all_files = self.conf_cmip.find_all_files_orig(**self._files_kwargs)


class ProcessorFromPost(Processor):
    def find_all_files(self):

        self.all_files = self.conf_cmip.find_all_files_post(**self._files_kwargs)
