import numpy as np

from filefinder import FileFinder
from utils.cmip_conf import _cmip_conf

# CONFIGURATION FILE

# =============================================================================
# Folders for the postprocessed data and figures
# =============================================================================

root_folder_postprocessed_data = "../data/"
root_folder_figures = "../figures/"
root_folder_warming_levels = "../warming_levels/"

# =============================================================================
# Reference Period
# =============================================================================

ANOMALY_YR_START = 1850
ANOMALY_YR_END = 1900

# =============================================================================
# CMIP5 Configuration
# =============================================================================


class _cmip5_conf(_cmip_conf):
    """docstring for cmip5_Conf"""

    def __init__(self):

        self._cmip = "cmip5"

        self._files_orig = FileFinder(
            path_pattern="/net/atmos/data/cmip5/{exp}/{table}/{varn}/{model}/{ens}/",
            file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{time}.nc",
        )

        self._files_post = FileFinder(
            path_pattern=root_folder_postprocessed_data + "cmip5/{varn}/{postprocess}/",
            file_pattern="{postprocess}_{varn}_{table}_{model}_{exp}_{ens}.nc",
        )

        self._files_fx = FileFinder(
            path_pattern="/net/atmos/data/cmip5/{exp}/fx/{varn}/{model}/r0i0p0/",
            file_pattern="{varn}_fx_{model}_{exp}_r0i0p0.nc",
        )

        self._figure_folder = root_folder_figures + "cmip5/cmip5_"
        self._warming_levels_folder = root_folder_warming_levels + "cmip5"

        self._hist_period = slice("1850", "2005")
        self._proj_period = slice("2006", "2100")

        self._scenarios_all = ["rcp26", "rcp45", "rcp60", "rcp85"]
        self._scenarios = ["rcp26", "rcp45", "rcp60", "rcp85"]

        self._ANOMALY_YR_START = ANOMALY_YR_START
        self._ANOMALY_YR_END = ANOMALY_YR_END


cmip5 = _cmip5_conf()

# =============================================================================
# CMIP6 Configuration
# =============================================================================


class _cmip6_conf(_cmip_conf):
    """docstring for cmip6_Conf"""

    def __init__(self):

        self._cmip = "cmip6"

        self._files_orig = FileFinder(
            path_pattern="/net/atmos/data/cmip6/{exp}/{table}/{varn}/{model}/{ens}/{grid}/",
            file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{grid}_{time}.nc",
        )

        self._files_post = FileFinder(
            path_pattern=root_folder_postprocessed_data + "cmip6/{varn}/{postprocess}/",
            file_pattern="{postprocess}_{varn}_{table}_{model}_{exp}_{ens}_{grid}.nc",
        )

        self._files_fx = FileFinder(
            path_pattern="/net/atmos/data/cmip6/{exp}/{table}/{varn}/{model}/{ens}/{grid}/",
            file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{grid}.nc",
        )

        self._figure_folder = root_folder_figures + "cmip6/cmip6_"
        self._warming_levels_folder = root_folder_warming_levels + "cmip6"

        self._hist_period = slice("1850", "2014")
        self._proj_period = slice("2015", "2100")

        self._scenarios_all = [
            "ssp119",
            "ssp126",
            "ssp245",
            "ssp370",
            "ssp434",
            "ssp460",
            "ssp585",
        ]
        self._scenarios = ["ssp119", "ssp126", "ssp245", "ssp370", "ssp585"]

        self._ANOMALY_YR_START = ANOMALY_YR_START
        self._ANOMALY_YR_END = ANOMALY_YR_END


cmip6 = _cmip6_conf()

# =============================================================================
# Scenarios - CMIP5
# =============================================================================

colors_rcp = dict(
    rcp85="#e31a1c", rcp60="#ff7f00", rcp45="#1f78b4", rcp26="#a6cee3", historical="0.1"
)

label_rcp = dict(
    rcp85="RCP8.5", rcp60="RCP6.0", rcp45="RCP4.5", rcp26="RCP2.6", historical="hist"
)

# =============================================================================
# Scenarios - CMIP6
# =============================================================================


# scens_ssp_all = ["historical"] + scens_ssp[:]

colors_ssp = dict(
    ssp119="#f1eef6",
    ssp126="#d0d1e6",
    ssp245="#a6bddb",
    ssp370="#74a9cf",
    ssp434="#3690c0",
    ssp460="#0570b0",
    ssp585="#034e7b",
    historical="0.1",
)

colors_ssp2 = dict(
    ssp119="#dadaeb",
    ssp126="#bcbddc",
    ssp434="#9e9ac8",
    ssp245="#807dba",
    ssp460="#6a51a3",
    ssp370="#54278f",
    ssp585="#3f007d",
    historical="0.1",
)


label_ssp = dict(
    ssp119="SSP1-1.9",
    ssp126="SSP1-2.6",
    ssp245="SSP2-4.5",
    ssp370="SSP3-7.0",
    ssp434="SSP4-3.4",
    ssp460="SSP4-6.0",
    ssp585="SSP5-8.5",
    historical="hist",
)


# =============================================================================
# Temperatures bins for the scaling plots
# =============================================================================

T_bins = np.arange(-0.5, 5.6, 0.5)
T_bin_centers = (T_bins[:-1] + T_bins[1:]) / 2

# =============================================================================
# 2.5° grid for regridding
# =============================================================================

# dest_grid_25 = xr.open_dataset('../data/dest_grid_25.nc')

# =============================================================================
# global warming levels
# =============================================================================

dTs = [1.5, 2, 3, 4]

# =============================================================================
# time periods for plots
# =============================================================================

time_periods = (
    slice(2020, 2039),
    slice(2040, 2059),
    slice(2060, 2079),
    slice(2080, 2099),
)

# =============================================================================
# scenarios
# =============================================================================

scens = ["rcp85", "rcp60", "rcp45", "rcp26", "historical"]
scens_no_hist = ["rcp85", "rcp60", "rcp45", "rcp26"]
