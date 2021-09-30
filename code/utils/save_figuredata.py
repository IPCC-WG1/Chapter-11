from datetime import datetime

import numpy as np
import xarray as xr

from utils import iav as iav_utils


class SaveFiguredata:
    def __init__(self, figure, units, chapter="Chapter 11", varn=None):
        """class to save out netCDF of data used to create the figures

        Parameters
        ----------
        figure : str
            Figure number (e.g. "Figure 11.3")
        units : str
            Units of displayed variable (e.g. "%")
        chapter : str, default: "Chapter 11"
            Chapter of the figure
        varn : str, optional
            If set overrides the default 'name' in the passed ds.

        Notes
        -----
        The approach taken here works but is not optimal. This should best be
        integrated in the function creating the figures. E.g. in `map_panel`
        we need to pass `average` and `hatch_simple` again.

        """

        self.author = "Mathias Hauser (mathias.hauser@env.ethz.ch)"
        self.report = "IPCC AR6 WGI"
        self.chapter = chapter
        self.figure = figure
        self.now = datetime.isoformat(datetime.now(), timespec="seconds")

        self.units = units
        self.varn = varn

    def _set_common_attrs(self, ds, panel=None):
        """set attrs common to all figure types"""

        ds.attrs["author"] = self.author
        ds.attrs["report"] = self.report
        ds.attrs["chapter"] = self.chapter
        ds.attrs["figure"] = self.figure
        if panel is not None:
            ds.attrs["panel"] = panel

        ds.attrs["creation_date"] = self.now

    def _to_ds(self, da):

        if self.varn is not None:
            da.name = self.varn

        return da.to_dataset()

    def map_panel(
        self,
        da,
        average,
        panel,
        warming_level,
        dim="mod_ens",
        hatch_simple=None,
    ):

        """save reduced variable used to create a map

        Parameters
        ----------
        da : xr.DataArray
            DataArray used for the plot.
        average : str
            Reduction function to apply.
        panel : str
            If not None indicate which panel of the
            figure the data represents.
        warming_level : float
            Global warming level of da.
        dim : str, default: "mod_ens"
            Dimension to average over.
        hatch_simple : float, default: None
            Indicate if the figure is hatched using the
            simple approach. Number indicates the fraction
            models that need to exhibit a change in the same
            direction.


        """

        # filter out inf & set to <NA>
        isinf = np.isinf(da)
        da = da.where(~isinf)

        da.attrs["units"] = self.units
        ds = self._to_ds(da)
        self._set_common_attrs(ds, panel=panel)

        ds.attrs["warming_level"] = f"{warming_level:0.1f}°C"

        ds.attrs["postprocess"] = ds.postprocess[0].values

        ds.attrs["varn_orig"] = da.varn[0].values
        ds.attrs["table"] = da.table[0].values
        ds.attrs["n_models"] = len(da.mod_ens)
        ds.attrs["model"] = da.model.values.tolist()
        ds.attrs["exp"] = da.exp.values.tolist()
        ds.attrs["ens"] = da.ens.values.tolist()

        with xr.set_options(keep_attrs=True):
            ds = getattr(ds, average)("mod_ens")

        if hatch_simple is not None:
            consistent_change = iav_utils._get_same_sign(da, hatch_simple, dim=dim)

            # we don't want to hatch Na
            all_notnull = da.notnull().all(dim)

            consistent_change = consistent_change.where(all_notnull)
            consistent_change.name = "consistent_change"
            consistent_change.attrs["comment"] = "for hatching (simple approach)"
            consistent_change.attrs["threshold"] = hatch_simple

            ds["consistent_change"] = consistent_change

        return ds

    def scaling(self, da, panels):

        da.attrs["units"] = self.units
        ds = self._to_ds(da)
        self._set_common_attrs(ds, panel=panels)

        ds.attrs["comment"] = (
            "Panel (a): shows the individual ensemble members and the median of three\n"
            "SSPs. Other panels show the multi model median (over the 'mod_ens'\n"
            "dimension). All data relative to 1850-1900. The regions 'global',\n"
            " 'ocean', 'land', 'GIC', 'EAN', and 'WAN' are not shown in the figure."
        )

        ds.wl.attrs["units"] = "°C"
        ds.wl.attrs["standard_name"] = "global_warming_level"
        ds.wl.attrs["long_name"] = "global warming level"
        ds.wl.attrs["comment"] = (
            "global warming level derived from global mean annual mean Near-Surface\n"
            "Air Temperature (Cross-Chapter Box 11.1)."
        )

        return ds
