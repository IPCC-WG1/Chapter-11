import os.path as path
import warnings

import numpy as np
import xarray as xr

import filefinder as ff

from . import computation
from .file_utils import _file_exists, mkdir
from .fx_files import _get_fx_data, _load_mask_or_weights
from .xarray_utils import mf_read_netcdfs

warnings.filterwarnings("ignore", message="variable '.*' has multiple fill values")


class _cmip_conf:
    """common configuration for cmip5 and cmip6"""

    def __init__(self):
        raise ValueError("Use 'conf.cmip5' of 'conf.cmip6' instead")

    # properties are defined in conf.py

    @property
    def cmip(self):
        """cmip version"""
        return self._cmip

    @property
    def files_orig(self):
        """FileFinder of the original, raw cmip data"""
        return self._files_orig

    @property
    def files_post(self):
        """FileFinder for the postprocessed cmip data"""
        return self._files_post

    @property
    def files_fx(self):
        """FileFinder for the fx files (e.g. land fraction)"""
        return self._files_fx

    @property
    def fixes_files(self):
        return self._fixes_files

    @property
    def fixes_data(self):
        return self._fixes_data

    @property
    def fixes_common(self):
        return self._fixes_common

    @property
    def figure_folder(self):
        return self._figure_folder

    @property
    def warming_levels_folder(self):
        return self._warming_levels_folder

    @staticmethod
    def _period_int(period):
        start = int(period.start)
        stop = int(period.stop)
        return slice(start, stop)

    @property
    def hist_period(self):
        return self._hist_period

    @property
    def hist_period_int(self):
        return self._period_int(self._hist_period)

    @property
    def proj_period(self):
        return self._proj_period

    @property
    def proj_period_int(self):
        return self._period_int(self._proj_period)

    @property
    def scenarios(self):
        return self._scenarios

    @property
    def scenarios_all(self):
        return self._scenarios_all

    @property
    def scenarios_incl_hist(self):
        return self._scenarios + ["historical"]

    @property
    def scenarios_all_incl_hist(self):
        return self._scenarios_all + ["historical"]

    @property
    def ANOMALY_YR_START(self):
        return self._ANOMALY_YR_START

    @property
    def ANOMALY_YR_END(self):
        return self._ANOMALY_YR_END

    def figure_filename(self, name, *subfolders, add_prefix=True):

        prefix = f"{self.cmip}_" if add_prefix else ""

        folders = (self.root_folder_figures, self.cmip) + subfolders

        return path.join(*folders, prefix + name)

    def load_orig(self, **metadata):

        folder_in = self.files_orig.create_path_name(**metadata)

        if "*" in folder_in:
            raise ValueError("'metadata' cannot contain wildcards")

        # maybe fix filename and glob files
        fNs_in = self.fixes_files(folder_in + "*")(metadata)

        # exit if the model is removed by the fixes
        if fNs_in is None:
            return []

        ds = mf_read_netcdfs(
            fNs_in,
            metadata=metadata,
            fixes=self.fixes_data,
            fixes_preprocess=self.fixes_common,
        )

        return ds

    _get_fx_data = _get_fx_data

    def load_fx(self, varn, meta, table="*", disallow_alternate=False):

        __, meta_fx = self._get_fx_data(
            varn, meta, table=table, disallow_alternate=disallow_alternate
        )

        if meta_fx:
            return self.load_orig(**meta_fx)[varn]
        return None

    _load_mask_or_weights = _load_mask_or_weights

    def load_mask(self, varn, meta, da=None):

        mask = self._load_mask_or_weights(varn, meta, da=da)
        
        # we want all gridpoints
        if mask is not None:
            mask = mask != 0
        
        return mask

    def load_weights(self, varn, meta, da=None):

        weights = self._load_mask_or_weights(varn, meta, da=da)

        # return weights in 0..1
        if weights is not None:
            weights = weights / 100

        return weights

    def load_post(self, **metadata):
        """load postprocessed data for a single scenario"""

        fN = self.files_post.create_full_name(**metadata)

        # error on missing file?
        if not _file_exists(fN):
            return []

        ds = xr.open_dataset(fN, decode_cf=False)

        # get rid of the "days" units, else CDD will have dtype = timedelta
        varn = metadata["varn"]
        units = ds[varn].attrs.get("units", None)
        if units in ["seconds", "days"]:
            ds[varn].attrs.pop("units")

        return xr.decode_cf(ds, use_cftime=True)

    def load_post_concat(self, **metadata):
        """combine historical simulation and projection

        Parameters
        ----------
        metadata : metadata
            Metadata idenrtifiying the simulation to load.

        ..note:: ``exp="historical"`` raises a ValueError
        use load_post(..., exp="historical") instead

        """

        exp = metadata.pop("exp")

        if exp == "historical":
            raise ValueError("Use 'load_post' to load historical exp")

        # load historical
        hist = self.load_post(exp="historical", **metadata)
        if not len(hist):
            self._not_found(exp="historical", **metadata)
            return []

        # cut to the historical period
        hist = hist.sel(time=self.hist_period)

        # load projection
        proj = self.load_post(exp=exp, **metadata)
        if not len(proj):
            self._not_found(exp=exp, **metadata)
            return []

        # cut to the projection period
        proj = proj.sel(time=self.proj_period)

        try:
            ds = xr.combine_by_coords(
                [hist, proj], join="exact", combine_attrs="override"
            )
        except (TypeError, ValueError) as e:
            print(metadata)
            raise e

        # combine
        return ds

    def load_post_all(
        self,
        varn,
        postprocess,
        exp=None,
        anomaly="absolute",
        at_least_until=None,
        year_mean=True,
        ensnumber=0,
        **metadata,
    ):
        """load postprocessed data for all models for a given scenario"""

        func = self.load_post

        return self._load_post_all_maybe_concat(
            varn=varn,
            postprocess=postprocess,
            exp=exp,
            anomaly=anomaly,
            at_least_until=at_least_until,
            year_mean=year_mean,
            ensnumber=ensnumber,
            func=func,
            **metadata,
        )

    def load_post_all_concat(
        self,
        varn,
        postprocess,
        exp=None,
        anomaly="absolute",
        at_least_until=2099,
        year_mean=True,
        ensnumber=0,
        **metadata,
    ):
        """load postprocessed data for all models concat for historical and scenario"""

        func = self.load_post_concat

        return self._load_post_all_maybe_concat(
            varn=varn,
            postprocess=postprocess,
            exp=exp,
            anomaly=anomaly,
            at_least_until=at_least_until,
            year_mean=year_mean,
            ensnumber=ensnumber,
            func=func,
            **metadata,
        )

    def find_all_files_orig(
        self,
        varn,
        exp=None,
        ensnumber=0,
        **metadata,
    ):
        # all tier1 acenarios inclusive hist
        scenarios = self.scenarios_incl_hist
        filefinder = self.files_orig.find_paths

        return self._find_all_files(
            scenarios,
            filefinder,
            varn=varn,
            exp=exp,
            ensnumber=ensnumber,
            **metadata,
        )

    def find_all_files_post(
        self,
        varn,
        postprocess,
        exp=None,
        ensnumber=0,
        **metadata,
    ):
        # all tier1 acenarios exclusive hist
        scenarios = self.scenarios
        filefinder = self.files_post.find_files

        return self._find_all_files(
            scenarios,
            filefinder,
            varn=varn,
            exp=exp,
            ensnumber=ensnumber,
            postprocess=postprocess,
            **metadata,
        )

    def _find_all_files(
        self,
        scenarios,
        filefinder,
        varn,
        exp=None,
        ensnumber=0,
        **metadata,
    ):

        if exp is None:
            exp = scenarios

        files = filefinder(varn=varn, exp=exp, **metadata)

        files = ff.cmip.ensure_unique_grid(files)
        files = ff.cmip.parse_ens(files)
        files = ff.cmip.create_ensnumber(files)
        files = files.search(ensnumber=ensnumber)

        return files

    def _load_post_all_maybe_concat(
        self,
        varn,
        postprocess,
        exp=None,
        anomaly="absolute",
        at_least_until=2099,
        year_mean=True,
        ensnumber=0,
        func=None,
        **metadata,
    ):

        files = self.find_all_files_post(
            varn=varn,
            postprocess=postprocess,
            exp=exp,
            ensnumber=ensnumber,
            **metadata,
        )

        output = list()

        for fN, metadata in files:
            ds = func(**metadata)
            # print(metadata)
            if ds and anomaly:
                ds = computation.calc_anomaly(
                    ds,
                    start=self.ANOMALY_YR_START,
                    end=self.ANOMALY_YR_END,
                    metadata=metadata,
                    how=anomaly,
                )

            # check if the dataset is long enough
            if ds and at_least_until:
                ds = computation.calc_anomaly(
                    ds,
                    start=at_least_until,
                    end=at_least_until,
                    metadata=metadata,
                    how="no_anom",
                )

            if ds and year_mean:
                ds = ds.groupby("time.year").mean("time")

            if ds:
                output.append((ds, metadata))

        return output

    @staticmethod
    def _not_found(**metadata):

        metadata = metadata.copy()

        # get rid of the ens labels
        metadata.pop("r", None)
        metadata.pop("i", None)
        metadata.pop("p", None)
        metadata.pop("f", None)

        msg = "-- no data found for: {}".format(metadata)
        print(msg)

    def _create_folder_for_output(self, files, postprocess_name):

        metadata = files[0][1].copy()

        metadata.pop("postprocess", None)
        folder_out = self.files_post.create_path_name(
            **metadata, postprocess=postprocess_name
        )
        mkdir(folder_out)

    def list_grid_resolutions_orig(
        self, varn="tas", exp="historical", ensnumber=0, table="Amon"
    ):
        """print resolution of individual models"""

        fc = self.find_all_files_orig(
            varn=varn, exp=exp, ensnumber=ensnumber, table=table
        )

        for fN, meta in fc:

            ds = self.load_orig(**meta)

            if len(ds) == 0:
                continue

            lat = np.round(ds.lat.diff("lat").values, 2)
            lon = np.round(ds.lon.diff("lon").values, 2)

            if np.allclose(lon[0], lon, atol=0.1):
                lon = f"{lon[0].item():0.2f}"
            else:
                lon = " var"

            if np.allclose(lat[0], lat, atol=0.1):
                lat = f"{lat[0].item():0.2f}"
            else:
                lat = " var"

            model = meta["model"] + ":"
            print(f"{model:<20} lat: {lat}, lon: {lon}")
