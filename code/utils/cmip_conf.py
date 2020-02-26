import xarray as xr

import filefinder as ff

from .file_utils import _file_exists
from . import computation


class _cmip_conf:
    """common configuration for cmip5 and cmip6"""

    def __init__(self):
        raise ValueError("Use 'conf.cmip5' of 'conf.cmip6' instead")

    # properties are defined in conf.py

    @property
    def cmip(self):
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

    def load_postprocessed(self, **metadata):
        """ load postprocessed data for a single scenario
        """

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

    def load_postprocessed_concat(self, **metadata):
        """combine historical simulation and projection

        Parameters
        ----------
        metadata : metadata
            Metadata idenrtifiying the simulation to load.

        ..note:: ``exp="historical"`` raises a ValueError
        use load_postprocessed(..., exp="historical") instead

        """

        exp = metadata.pop("exp")

        if exp == "historical":
            raise ValueError("Use 'load_postprocessed' to load historical exp")

        # load historical
        hist = self.load_postprocessed(exp="historical", **metadata)
        if not len(hist):
            self._not_found(exp="historical", **metadata)
            return []

        # cut to the historical period
        hist = hist.sel(time=self.hist_period)

        # load projection
        proj = self.load_postprocessed(exp=exp, **metadata)
        if not len(proj):
            self._not_found(exp=exp, **metadata)
            return []

        # cut to the projection period
        proj = proj.sel(time=self.proj_period)

        # combine
        return xr.concat([hist, proj], dim="time", compat="override", coords="minimal")

    def load_postprocessed_all(
        self,
        varn,
        postprocess,
        exp,
        anomaly="absolute",
        year_mean=True,
        ensnumber=0,
        **metadata,
    ):
        """load postprocessed data for all models for a given scenario"""

        func = self.load_postprocessed

        return self._load_postprocessed_all_maybe_concat(
            varn=varn,
            postprocess=postprocess,
            exp=exp,
            anomaly=anomaly,
            year_mean=year_mean,
            ensnumber=ensnumber,
            func=func,
            **metadata,
        )

    def load_postprocessed_all_concat(
        self,
        varn,
        postprocess,
        exp=None,
        anomaly="absolute",
        year_mean=True,
        ensnumber=0,
        **metadata,
    ):
        """load postprocessed data for all models concat for historical and scenario"""

        func = self.load_postprocessed_concat

        return self._load_postprocessed_all_maybe_concat(
            varn=varn,
            postprocess=postprocess,
            exp=exp,
            anomaly=anomaly,
            year_mean=year_mean,
            ensnumber=ensnumber,
            func=func,
            **metadata,
        )

    def _load_postprocessed_all_maybe_concat(
        self,
        varn,
        postprocess,
        exp=None,
        anomaly="absolute",
        year_mean=True,
        ensnumber=0,
        func=None,
        **metadata,
    ):

        if exp is None:
            exp = self.scenarios

        files = self.files_post.find_files(
            varn=varn, postprocess=postprocess, exp=exp, **metadata
        )

        files = ff.cmip.parse_ens(files)
        files = ff.cmip.create_ensnumber(files)
        files = files.search(ensnumber=ensnumber)

        output = list()

        for fN, metadata in files:
            # print(fN, metadata)
            ds = func(**metadata)

            if ds and anomaly:
                ds = computation.calc_anomaly(
                    ds,
                    start=self.ANOMALY_YR_START,
                    end=self.ANOMALY_YR_END,
                    metadata=metadata,
                    how=anomaly,
                )

            if ds and year_mean:
                ds = ds.groupby("time.year").mean("time")

            if ds:
                output.append((ds, metadata))

        return output

    def _not_found(self, **metadata):

        msg = "-- no data found for: {}".format(metadata)
        print(msg)
