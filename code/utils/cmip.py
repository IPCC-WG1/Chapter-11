import xarray as xr
from .file_utils import _file_exists


class _cmip_conf:
    """docstring for cmip5_Conf"""

    def __init__(self):
        raise ValueError("Use 'conf.cmip5' of 'conf.cmip6' instead")

    @property
    def files_orig(self):
        """FileFinder of the original, raw cmip data"""
        return self._files_orig

    @property
    def files_post(self):
        """FileFinder for the postprocessed cmip data"""
        return self._files_post

    @property
    def hist_period(self):
        return self._hist_period

    @property
    def proj_period(self):
        return self._proj_period

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

    def load_postprocessed(self, **metadata):

        fN = self.files_post.create_full_name(**metadata)

        # no error on missing file?
        if not _file_exists(fN):
            return []

        return xr.open_dataset(fN, use_cftime=True)

    def load_postprocessed_concat(self, **metadata):
        """combine historical simulation and projection

        Parameters
        ----------
        metadata : metadata
            Metadata idenrtifiying the simulation to load.

        ..note:: "exp" can not be historical

        """

        exp = metadata.pop("exp")

        msg = f"Use 'load_postprocessed' to load historical scen"
        assert exp != "historical", msg

        # load historical
        hist = self.load_postprocessed(exp="historical", **metadata)
        if not len(hist):
            self._not_found(exp="historical", **metadata)
            return []

        hist = hist.sel(time=self.hist_period)

        # load projection
        proj = self.load_postprocessed(exp=exp, **metadata)
        if not len(proj):
            self._not_found(exp=exp, **metadata)
            return []

        proj = proj.sel(time=self.proj_period)

        # combine
        return xr.concat([hist, proj], dim="time", compat="override", coords="minimal")

    def _not_found(self, **metadata):

        msg = "-- no data found for: {}".format(metadata)
        print(msg)
