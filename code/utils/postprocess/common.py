import time

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
            print(f"processing {i + 1} of {n_to_process} ({now})")
            print(f"- {meta}")
            print(f"- {file}")

            yield file, meta

    def transform(self, **kwargs):

        for file, meta in self._yield_transform(**kwargs):

            ds = self._transform(**meta)
            fN_out = self.fN_out(**meta)
            self.save(ds, fN_out)

    def save(self, ds, fN_out):

        # make folder

        if ds is None:
            raise ValueError("Forgot to return ds?")

        if len(ds) != 0:
            ds.to_netcdf(fN_out, format="NETCDF4_CLASSIC")

    def get_lat_weights(self, fx_weights, meta, ds):
        if fx_weights and len(ds):
            weights = self.conf_cmip.get_weights(fx_weights, meta, ds)

            # "latitude" means there are 2D coords
            if weights is None and "latitude" not in ds.coords:
                weights = xru.cos_wgt(ds)

            return weights

    def get_land_mask(self, meta, da=None):

        mask = self.conf_cmip.load_mask("sftlf", meta, da)

        if mask is None:
            mask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            mask = mask.squeeze(drop=True)

        return mask

    def get_ocean_mask(self, meta, da=None):

        return ~self.get_land_mask(meta, da=da)

    def get_landice_mask(self, meta, da=None):

        return self.conf_cmip.load_mask("sftgif", meta, da)

    def get_masks(self, masks, meta, da=None):

        if issubclass(type(da), list):
            return

        if isinstance(masks, str):
            masks = [masks]
        elif masks is None:
            masks = []

        # initialize as no mask
        mask = False

        for m in masks:
            other = getattr(self, f"get_{m}_mask")(meta, da)
            if other is not None:
                mask = mask | other

        if mask is False:
            mask = None

        return mask

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