import os.path as path
import warnings

import numpy as np
import xarray as xr

import filefinder as ff

from . import computation
from . import xarray_utils as xru
from .file_utils import _file_exists, mkdir
from .fx_files import _find_fx_files, _load_mask_or_weights

warnings.filterwarnings("ignore", message="variable '.*' has multiple fill values")


class _cmip_conf:
    """common configuration for cmip5 and cmip6

    This class abstracts differences between cmip5 and cmip6 away

    """

    def __init__(self):
        raise NotImplementedError("Use 'conf.cmip5' of 'conf.cmip6' instead")

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
        """fixes on the original cmip file paths, e.g. removing a simulation entirely"""
        return self._fixes_files

    @property
    def fixes_data(self):
        """fixes on the original cmip data, e.g. correct the sign, or calendar"""
        return self._fixes_data

    @property
    def fixes_preprocess(self):
        """fixes on the original cmip data in the prepocessor, e.g. coordinate names"""
        return self._fixes_preprocess

    @property
    def figure_folder(self):
        return self._figure_folder

    @property
    def warming_levels_folder(self):
        return self._warming_levels_folder

    @staticmethod
    def _period_int(period):
        """convert slice with str start and stop to one with int elements"""
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
        """list of core scenarios"""
        return self._scenarios

    @property
    def scenarios_all(self):
        """list of all scenarios"""
        return self._scenarios_all

    @property
    def scenarios_incl_hist(self):
        """list of core scenarios inclusive historical"""
        return self._scenarios + ["historical"]

    @property
    def scenarios_all_incl_hist(self):
        """list of all scenarios inclusive historical"""
        return self._scenarios_all + ["historical"]

    @property
    def ANOMALY_YR_START(self):
        return self._ANOMALY_YR_START

    @property
    def ANOMALY_YR_END(self):
        return self._ANOMALY_YR_END

    def figure_filename(self, name, *subfolders, add_prefix=True):
        """
        create filenames for figures, relative to root_folder_figures (../figures)

        Parameters
        ----------
        name : str
            File name of the figure
        subfolders : list of str
            Folders of the figure.
        add_prefix : bool, default True
            If True adds 'cmip6_' in front of the filename.
        """

        prefix = f"{self.cmip}_" if add_prefix else ""

        folders = (self.root_folder_figures, self.cmip) + subfolders

        return path.join(*folders, prefix + name)

    def load_orig(self, check_time=True, **meta):
        """load original (raw) cmip data from the ETH archive on /net/atmos/data/

        Parameters
        ----------
        check_time, bool, default: True
            If true checks the loaded data for errors in the time axis (missing time
            steps etc.). If false the check is bypassed.
        metadata : kwargs
            Keys to select the models simulation to be loaded. Includes 'varn', 'model',
            'ens', etc.

        Returns
        -------
        ds : xr.Dataset
            Dataset of the specified cmip data.
        """

        folder_in = self.files_orig.create_path_name(**meta)

        if "*" in folder_in:
            raise ValueError("'meta' cannot contain wildcards")

        # maybe fix filename and glob files
        fNs_in = self.fixes_files(folder_in + "*", meta)

        # exit if the model is removed by the fixes
        if fNs_in is None:
            print("- model manually removed in 'fixes_files'")
            return []

        # read, concatenate and fix files
        ds = xru.open_mfdataset(
            fNs_in,
            metadata=meta,
            fixes=self.fixes_data,
            fixes_preprocess=self.fixes_preprocess,
            check_time=check_time,
        )

        return ds

    # add _find_fx_files as method
    _find_fx_files = _find_fx_files

    def load_fx(self, varn, meta, table="*", disallow_alternate=False):
        """load time-constant files, e.g. land fraction

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

        __, meta_fx = self._find_fx_files(
            varn, meta, table=table, disallow_alternate=disallow_alternate
        )

        if meta_fx:
            return self.load_orig(**meta_fx)[varn]
        return None

    # add _load_mask_or_weights as method
    _load_mask_or_weights = _load_mask_or_weights

    def load_mask(self, varn, meta, da=None):
        """load fx file as boolean mask

        Parameters
        ----------
        varn : str
            Variable name to load.
        meta : dict of metadata
            Metadata of the model to load the fx files for. Note incompatible metadata
            (e.g. "varn") will be automatically removed from meta.
        da : xr.DataArray, optional
            DataArray to align the mask with. Used to check if the grid of da and the
            mask is compatible.

        Returns
        -------
        mask : xr.DataArray or None
            Returns the mask as DataArray if found, else returns None.

        Notes
        -----
        All gridpoints with values > 0 are in the mask. We want to include all grid
        points that have a fractional area, such that it can be taken into account
        with the weights.
        """

        mask = self._load_mask_or_weights(varn, meta, da=da)

        # we want all gridpoints
        if mask is not None:
            mask = mask != 0

        return mask

    def load_weights(self, varn, meta, da=None):
        """load fx file as fractional (0..1) weights

        Parameters
        ----------
        varn : str
            Variable name to load.
        meta : dict of metadata
            Metadata of the model to load the fx files for. Note incompatible metadata
            (e.g. "varn") will be automatically removed from meta.
        da : xr.DataArray, optional
            DataArray to align the weights with. Used to check if the grid of da and the
            weights is compatible.

        Returns
        -------
        weights : xr.DataArray or None
            Returns the weights as DataArray if found, else returns None.

        Notes
        -----
        weights are given as fraction, i.e. in the range 0..1
        """
        weights = self._load_mask_or_weights(varn, meta, da=da)

        # return weights in 0..1
        if weights is not None:
            weights = weights / 100

        return weights

    def load_iav(self, iav="IAV20", **meta):
        """load interannual variability

        Parameters
        ----------
        iav : str, default: "IAV20"
            Which interannual variability variant to load.
        meta : kwargs
            Keys to select the models simulation to be loaded. Includes 'varn', 'model',
            'ens', etc.

        Notes
        -----
        As Chapter 11 uses the simple method to hatch the maps this is not used.
        """

        postprocess = meta.pop("postprocess")
        meta.pop("exp", None)

        return self.load_post_all(
            postprocess=f"{postprocess}_{iav}",
            exp="piControl",
            anomaly=None,
            at_least_until=None,
            year_mean=False,
            **meta,
        )

    def load_post(self, **metadata):
        """load postprocessed data for a single simulation

        Parameters
        ----------
        meta : kwargs
            Keys to select the models simulation to be loaded. Includes 'varn', 'model',
            'ens', etc.

        Returns
        -------
        ds : xr.Dataset
            Dataset of the specified, postprocessed cmip data.
        """

        fN = self.files_post.create_full_name(**metadata)

        if not _file_exists(fN):
            return []

        ds = xr.open_dataset(fN, decode_cf=False)

        # get rid of the "days" units, else CDD will have dtype = timedelta
        varn = metadata["varn"]
        if varn in ds.data_vars:
            units = ds[varn].attrs.get("units", None)
            if units in ["seconds", "days"]:
                ds[varn].attrs.pop("units")

        return xr.decode_cf(ds, use_cftime=True)

    def load_post_concat(self, **metadata):
        """
        load postprocessed data for a single simulation, combines historical simulation
        and projection

        Parameters
        ----------
        metadata : metadata
            Metadata idenrtifiying the simulation to load.

        Notes
        ------
        This function concatenates historical data and projections. To load historical
        data use ``load_post(..., exp="historical")`` instead.
        """

        exp = metadata.pop("exp")

        if exp == "historical":
            raise ValueError("Use 'load_post' to load historical exp")

        # load historical
        hist = self.load_post(exp="historical", **metadata)
        if not len(hist):
            self._print_meta(exp="historical", **metadata)
            return []

        # cut to the historical period
        hist = hist.sel(time=self.hist_period)

        # load projection
        proj = self.load_post(exp=exp, **metadata)
        if not len(proj):
            self._print_meta(exp=exp, **metadata)
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
        """
        load postprocessed data for all models for a given scenario, without concatenation

        Parameters
        ----------
        varn : str
            Variable to load.
        postprocess : str
            Name of the applied postprocessing.
        exp : str, default: None
            Which scenarios to load. Per default (None) loads `self.scenarios_incl_hist`
            i.e. core scenarios inclusive the historical simulation.
        anomaly : str, default: "absolute"
            If and what anomaly to calculate (see ``utils.computation.calc_anomaly``).
            The anomaly is computed w.r.t. self.ANOMALY_YR_START and self.ANOMALY_YR_END
            i.e. 1850-1900. If None, no anomaly is computed. Note that ``calc_anomaly``
            also checks if the historical spans the anomaly period and if not the model
            simulation is discarded.
        at_least_until : int, default None
            If not None checks if the simulation extends to at least the indicated year,
            and if not the model simulation is discarded.
        ensnumber : int or None, default 0
            Which ensemble numbers to load. None loads all available members.
        metadata : kwargs, optional
            Keys to more specifically select the models simulation to be loaded.
            Includes 'model', 'ens', etc.

        Returns
        -------
        data : DataList
            List of xr.Dataset objects and dict of metadata of the loaded data.
        """

        return self._load_post_all_maybe_concat(
            varn=varn,
            postprocess=postprocess,
            exp=exp,
            anomaly=anomaly,
            at_least_until=at_least_until,
            year_mean=year_mean,
            ensnumber=ensnumber,
            func=self.load_post,
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
        """
        load postprocessed data for all models and concatenate the data for historical
        and scenario

        Parameters
        ----------
        varn : str
            Variable to load.
        postprocess : str
            Name of the applied postprocessing.
        exp : str, default: None
            Which scenarios to load. Per default (None) loads `self.scenarios` i.e. all
            core scenarios exclusive the historical simulation.
        anomaly : str, default: "absolute"
            If and what anomaly to calculate (see ``utils.computation.calc_anomaly``).
            The anomaly is computed w.r.t. self.ANOMALY_YR_START and self.ANOMALY_YR_END
            i.e. 1850-1900. If None, no anomaly is computed. Note that ``calc_anomaly``
            also checks if the historical spans the anomaly period and if not the model
            simulation is discarded.
        at_least_until : int, default 2099
            If not None checks if the simulation extends to at least the indicated year,
            and if not the model simulation is discarded.
        ensnumber : int or None, default 0
            Which ensemble numbers to load. None loads all available members.
        metadata : kwargs, optional
            Keys to more specifically select the models simulation to be loaded.
            Includes 'model', 'ens', etc.

        Returns
        -------
        data : DataList
            List of xr.Dataset objects and dict of metadata of the loaded data.

        """

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

    _filefinder_find_all_files_orig_ = None

    @property
    def _filefinder_find_all_files_orig(self):

        if self._filefinder_find_all_files_orig_ is None:
            return self.files_orig.find_paths

        return self._filefinder_find_all_files_orig_

    def find_all_files_orig(
        self,
        varn,
        exp=None,
        ensnumber=0,
        **metadata,
    ):
        """
        find all simulations of the original (raw) cmip data from the ETH archive on
        /net/atmos/data/ for given conditions

        Parameters
        ----------
        varn : str
            Variable name.
        exp : str or list of str, default None
            Defines which scenarios to load. Per default (None) loads
            `self.scenarios_incl_hist` i.e. core scenarios inclusive the historical
            simulation.
        ensnumber : int or None, default 0
            Which ensemble numbers to load. None loads all available members.
        metadata : kwargs, optional
            Keys to more specifically select the models simulation to be loaded.
            Includes 'model', 'ens', etc.
        """

        # all core acenarios inclusive hist
        scenarios = self.scenarios_incl_hist
        filefinder = self._filefinder_find_all_files_orig

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
        """find all simulations of postprocessed data for given conditions

        Parameters
        ----------
        varn : str
            Variable name.
        postprocess : str
            Name of the applied postprocessing.
        exp : str or list of str, default None
            Defines which scenarios to load. Per default (None) loads `self.scenarios` i.e.
            core scenarios exclusive the historical simulation.
        ensnumber : int or None, default 0
            Which ensemble numbers to load. None loads all available members.
        metadata : kwargs, optional
            Keys to more specifically select the models simulation to be loaded.
            Includes 'model', 'ens', etc.
        """

        # all coer acenarios exclusive hist
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

        if self._cmip != "cmip6_ng":
            files = ff.cmip.ensure_unique_grid(files)
        files = ff.cmip.parse_ens(files)

        keys = None if self._cmip != "cmip6_ng" else ["exp", "varn", "model"]
        files = ff.cmip.create_ensnumber(files, keys=keys)
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
    def _print_meta(**metadata):
        """print metadata"""

        metadata = metadata.copy()

        # get rid of the ens labels
        metadata.pop("r", None)
        metadata.pop("i", None)
        metadata.pop("p", None)
        metadata.pop("f", None)

        msg = "-- no data found for: {}".format(metadata)
        print(msg)

    def _create_folder_for_output(self, files, postprocess_name):
        """create a folder for saving postprocessed data"""

        metadata = files[0][1].copy()
        metadata.pop("postprocess", None)
        folder_out = self.files_post.create_path_name(
            **metadata, postprocess=postprocess_name
        )
        mkdir(folder_out)

    def list_grid_resolutions_orig(
        self, varn="tas", exp="historical", ensnumber=0, table="Amon"
    ):
        """print resolution of individual models

        Parameters
        ----------
        varn : str, default: "tas"
            Variable name.
        exp : str, default: "historical"
            Name of scenario.
        ensnumber : int, default: 0
            Ensemble number.
        table : str, default: "Amon"
            Table name.
        """

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
