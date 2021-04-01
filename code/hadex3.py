import warnings

import cartopy.crs as ccrs
import cartopy.feature as cfeatures
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import mplotutils as mpu
import xarray as xr

import filefinder as ff
from utils import plot
from utils.statistics import theil_ufunc

# we would get 13 warnings when reading HadEX3 data
warnings.filterwarnings("ignore", message="variable '.*' has multiple fill values")

CURRENT_VERSION = "3.0.2"


class HadEx3_cls:
    """docstring for HadEx3_cls."""

    def __init__(self):

        self._files_raw = ff.FileFinder(
            path_pattern="../data/HadEX3/v{version}/raw/",
            file_pattern="HadEX3_{varn}_{year_from}-{year_to}_ADW_{climatology}_1.25x1.875deg.nc",
        )

        self._files_post = ff.FileFinder(
            path_pattern="../data/HadEX3/v{version}/{postprocess}/",
            file_pattern="{postprocess}_HadEX3_{varn}_ADW_{climatology}.{ending}",
        )

        self._all_files_raw = None

        self.map_abbrevs = dict(
            TXx="maximum Tmax",
            TXn="minimum Tmax",
            TNx="maximum Tmin",
            TNn="minimum Tmin",
            TX90p="warm days",
            TX10p="cool days",
            TN90p="warm nights",
            TN10p="cool nights",
            TR="tropical nights",
            SU="summer days",
            FD="frost days",
            ID="ice days",
            CSDI="cool spell duration",
            WSDI="warm spell duration",
            DTR="diurnal temperature range",
            GSL="growing season length",
            CDD="consecutive dry days",
            CWD="consecutive wet days",
            PRCPTOT="total precipitation",
            R10mm="precip in &gt;10mm days",
            R20mm="precip in &gt;20mm days",
            Rx1day="maximum 1 day total",
            Rx5day="maximum 5 day total",
            R95p="amount in very wet days",
            R99p="amount in extremely wet days",
            R95pTOT="fraction in very wet days",
            R99pTOT="fraction in extremely wet days",
            SDII="specific daily intensity",
        )

    @property
    def files_raw(self):
        """FileFinder for raw HadEx3 files"""
        return self._files_raw

    @property
    def files_post(self):
        """FileFinder for postprocessed HadEx3 files"""
        return self._files_post

    @property
    def all_files_raw(self):
        """FileFinder list of all raw files"""
        if self._all_files_raw is None:
            self._all_files_raw = self.files_raw.find_files()

        return self._all_files_raw

    def __repr__(self):
        return "<HadEx3 class>"

    def _read_file(
        self, varn, climatology="61-90", variable="Ann", version=CURRENT_VERSION
    ):
        """read one file and return with metadata

        Parameters
        ==========
        varn : str
            Variable name (e.g. 'TXx')
        climatology: "61-90" | "81-10"
            Climatology of the HadEx files
        vaiable : str
            Name of the variable to read, e.g. "Ann",
            "JAN"

        Returns
        =======
        da : xr.DataArray
        meta : dict of meta data
        """

        fc = self.all_files_raw
        fN, meta = fc.search(varn=varn, climatology=climatology, version=version)[0]

        ds = xr.open_dataset(fN, decode_cf=False)

        ds = ds.rename(longitude="lon", latitude="lat")

        # get rid of the "days" units, else CDD will have dtype = timedelta
        units = ds[variable].attrs.get("units", None)
        if units in ["seconds", "days"]:
            ds[variable].attrs.pop("units")

        ds = xr.decode_cf(ds, use_cftime=True)

        da = ds[variable]

        return da, meta

    def read_file(
        self, varn, climatology="61-90", variable="Ann", version=CURRENT_VERSION
    ):
        """read one file and return without metadata

        Parameters
        ==========
        varn : str
            Variable name (e.g. 'TXx')
        climatology: "61-90" | "81-10"
            Climatology of the HadEx files
        vaiable : str
            Name of the variable to read, e.g. "ANN",
            "JAN"

        Returns
        =======
        da : xr.DataArray
        """

        ds, meta = self._read_file(
            varn, climatology=climatology, variable=variable, version=version
        )

        return ds

    def read_files(
        self, varns, climatology="61-90", variable="Ann", version=CURRENT_VERSION
    ):
        """read several files

        Parameters
        ==========
        varn : str
            Variable name (e.g. 'TXx')
        climatology: "61-90" | "81-10"
            Climatology of the HadEx files
        vaiable : str
            Name of the variable to read, e.g. "Ann",
            "Jan"

        Returns
        =======
        filelist : list with da/ meta data structure
        """
        if isinstance(varns, str):
            varns = [varns]

        out = list()

        for varn in varns:
            ds, meta = self._read_file(
                varn=varn, climatology=climatology, variable=variable, version=version
            )

            out.append([ds, meta])

        return out

    def read_landmask(self, version=CURRENT_VERSION):
        """read the HadEx3 landmask"""

        landmask, meta = self._read_file(
            varn="landmask", climatology=None, variable="landmask", version=version
        )

        return landmask


# ds = ds.rename(longitude="lon", latitude="lat")


HadEx3 = HadEx3_cls()


def _invalidated(valid, condition, what=""):
    """print percentage of invalidated grid cells"""
    n_valid = valid.sum().values

    invalidated = valid & (~condition)

    invalidated = invalidated.values.sum() / n_valid * 100

    print(f"{what} removed {invalidated:0.2f} % valid gridpoints")


def find_valid_gridpoints_dunn(
    da, time=slice(1950, 2018), last_timestep=2009, minimum_valid=0.66
):
    """find valid grid points after Dunn et al., Figure 2a

    1.) time 1950...2018
    2.) last valid data must at least be in 2009
    3.) 66% of valid gridpoints
    """

    # select timeframe
    da = da.sel(time=time)

    # find valid data
    isnull = da.isnull()
    notnull = ~isnull

    # grid cells with at least one datapoint
    atleast_one = notnull.any("time")

    # last valid data must be after 2009
    if last_timestep is not None:
        # find the last timestep that is non-nan
        idx = isnull.sortby("time", ascending=False).argmin("time")
        last_timestep_in_series = da.time.max() - idx

        condition = last_timestep_in_series >= last_timestep

        da = da.where(condition)

        _invalidated(atleast_one, condition, what="end date")

    # require more than minimum_valid timesteps
    valid_fraction = notnull.sum("time") / len(da.time)
    condition = valid_fraction >= minimum_valid

    da = da.where(condition)

    _invalidated(atleast_one, condition, what="minimum_valid")

    return da


def valid_for_globmean(da, time=slice(1950, 2018), minimum_valid=0.9):
    # for the global mean Dunn et al. require at least 90% valid years (see Figure 2.)

    # select timeframe
    da = da.sel(time=time)

    # find valid data
    isnull = da.isnull()
    notnull = ~isnull

    # grid cells with at least one datapoint
    atleast_one = notnull.any("time")

    # require more than minimum_valid timesteps
    valid_fraction = notnull.sum("time") / len(da.time)
    condition = valid_fraction >= minimum_valid

    da = da.where(condition)

    _invalidated(atleast_one, condition, what="minimum_valid")

    return da


def theil_after_dunn(
    da, last_timestep=2009, alpha=0.05, time=slice(1950, 2018), minimum_valid=0.66
):
    """calculate theil-sen slope with the same conditions as in the dunn et al paper

    period: 1950...2018
    data needs to go at least until 2009
    at least 66% of valid data
    """

    da_valid = find_valid_gridpoints_dunn(
        da, time=time, last_timestep=last_timestep, minimum_valid=minimum_valid
    )

    return theil_ufunc(da_valid, dim="time", alpha=alpha)


def plot_theilslope(
    theil_slope,
    theil_sign,
    ax,
    title=None,
    add_colorbar=True,
    colorbar_kwargs=None,
    stippling_label="Non-significant",
    **kwargs,
):
    """plot theil slope and significance"""

    no_data_color = "0.8"
    land_kws = dict(fc=no_data_color, ec="none")

    if colorbar_kwargs is None:
        colorbar_kwargs = {}

    # coastline_kws = dict(color="0.1", lw=1, zorder=1.2)
    # ax.coastlines(**coastline_kws)
    #
    # ax.add_feature(cfeatures.LAND, fc=no_data_color, ec="none")

    # h = ().plot(
    #     ax=ax, transform=ccrs.PlateCarree(), add_colorbar=False, **kwargs
    # )

    h = plot.one_map_flat(
        theil_slope * 10,
        ax=ax,
        mask_ocean=True,
        ocean_kws=None,
        add_coastlines=True,
        coastline_kws=None,
        add_land=True,
        land_kws=land_kws,
        **kwargs,
    )

    lh1 = plot.text_legend(ax, "Color", "Significant", size=7)

    lh2 = plot.hatch_map(
        ax,
        theil_sign,
        "cccc",
        label=stippling_label,
        invert=True,
        linewidth=0.1,
        color="0.1",
    )

    lh2 = mpatches.Patch(
        facecolor="none",
        ec="0.1",
        lw=0.5,
        hatch="ccc",
        label=stippling_label,
    )

    lh3 = mpatches.Patch(fc=no_data_color, ec="0.2", label="No data", lw=0.5)
    # legend_handle = [lh1, (lh2, lh3), lh3], [lh1.get_label(), lh2.get_label(), lh3.get_label()]

    legend_handle = [lh1, lh2, lh3]

    cbar = h
    if add_colorbar:
        cbar = mpu.colorbar(h, ax, orientation="horizontal", **colorbar_kwargs)
        cbar.ax.tick_params(labelsize=9)

    # ax.set_global()

    if title is not None:
        ax.set_title(title, size=8)

    return cbar, legend_handle


def delta_after_dunn(
    da, last_timestep=2009, alpha=0.05, time=slice(1950, 2018), minimum_valid=0.66
):
    """calculate delta"""

    da_valid = find_valid_gridpoints_dunn(
        da, time=time, last_timestep=last_timestep, minimum_valid=minimum_valid
    )

    d_recent = da_valid.sel(time=slice(1981, 2010)).mean("time")
    d_past = da_valid.sel(time=slice(1951, 1980)).mean("time")

    delta = d_recent - d_past

    return delta


def plot_delta(delta, ax, title=None, **kwargs):
    """plot theil slope and significance"""

    ax.coastlines()

    ax.add_feature(cfeatures.LAND, color="0.8")

    h = delta.plot(ax=ax, transform=ccrs.PlateCarree(), add_colorbar=False, **kwargs)

    cbar = mpu.colorbar(h, ax, orientation="horizontal")
    cbar.ax.tick_params(labelsize=8)

    ax.set_global()

    if title is not None:
        ax.set_title(title, size=8)


def plot_trend_diff(theil_slope, theil_sign, delta, varn):

    f, axes = plt.subplots(1, 2, subplot_kw=dict(projection=ccrs.Robinson()))

    ax = axes[0]

    #     levels = [-2, -1, -0.5, -0.25, 0, 0.25, 0.5, 1, 2]

    plot_theilslope(
        theil_slope,
        theil_sign,
        ax=ax,
        robust=True,
        cmap="RdYlBu_r",
        extend="both",
        title=f"{varn} - Ann, linear trend 1950-2018",
    )

    #     ar6_land.plot_regions(ax=ax, add_label=False, line_kws=dict(lw=1))

    ax = axes[1]

    plot_delta(
        delta, ax=ax, title=f"{varn} - Ann, (1981, 2010)-(1951, 1980)", robust=True
    )
    #     ar6_land.plot_regions(ax=ax, add_label=False, line_kws=dict(lw=1))

    f.subplots_adjust(wspace=0.05)

    mpu.set_map_layout(axes)

    return axes
