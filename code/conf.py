import os.path as path

import numpy as np

import fixes
from filefinder import FileFinder
from utils.cmip_conf import _cmip_conf

# CONFIGURATION FILE

# =============================================================================
# Folders for the postprocessed data and figures
# =============================================================================

root_folder_postprocessed_data = "../data/"
root_folder_figures = "../figures/"


def figure_filename(name, *subfolders):
    """create filenames for figures

    Parameters
    ----------
    name : str
        File name of the figure
    *subfolders : list of str
        Folders of the figure.

    """

    folders = (root_folder_figures,) + subfolders

    return path.join(*folders, name)


# =============================================================================
# Reference Period
# =============================================================================

ANOMALY_YR_START = 1850
ANOMALY_YR_END = 1900

# =============================================================================
# Colors
# =============================================================================

# https://github.com/IPCC-WG1/colormaps/blob/master/categorical_colors_rgb_0-255/ssp_cat_2.txt
COLORS_SSP = {
    "ssp119": np.array([30, 150, 132]) / 255,
    "ssp126": np.array([29, 51, 84]) / 255,
    "ssp245": np.array([234, 221, 61]) / 255,
    "ssp370": np.array([242, 17, 17]) / 255,
    "ssp370low": np.array([242, 17, 17]) / 255,
    "ssp434": np.array([99, 189, 229]) / 255,
    "ssp460": np.array([232, 136, 49]) / 255,
    "ssp534os": np.array([154, 109, 201]) / 255,
    "ssp585": np.array([132, 11, 34]) / 255,
}

# =============================================================================
# CMIP5 Configuration
# =============================================================================


class _cmip5_conf(_cmip_conf):
    """configuration for cmip5 archive and postprocessed files"""

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
            path_pattern="/net/atmos/data/cmip5/{exp}/{table}/{varn}/{model}/{ens}/",
            file_pattern="{varn}_{table}_{model}_{exp}_{ens}.nc",
        )

        self._fixes_files = fixes.cmip5_files
        self._fixes_data = fixes.cmip5_data
        self._fixes_preprocess = fixes.cmip5_preprocess

        self._figure_folder = root_folder_figures + "cmip5/cmip5_"
        self.root_folder_figures = root_folder_figures

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
    """configuration for cmip6 archive and postprocessed files"""

    def __init__(self):

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

        self._fixes_files = fixes.cmip6_files
        self._fixes_data = fixes.cmip6_data
        self._fixes_preprocess = fixes.cmip6_preprocess

    # define some variables on class level so they don't need to be duplicated for
    # cmip6-ng

    _cmip = "cmip6"

    _figure_folder = root_folder_figures + "cmip6/cmip6_"
    root_folder_figures = root_folder_figures

    _hist_period = slice("1850", "2014")
    _proj_period = slice("2015", "2100")

    _scenarios_all = [
        "ssp119",
        "ssp126",
        "ssp245",
        "ssp370",
        "ssp434",
        "ssp460",
        "ssp585",
    ]
    _scenarios = ["ssp119", "ssp126", "ssp245", "ssp370", "ssp585"]

    _ANOMALY_YR_START = ANOMALY_YR_START
    _ANOMALY_YR_END = ANOMALY_YR_END

    colors = COLORS_SSP


cmip6 = _cmip6_conf()


class _cmip6_ng_conf(_cmip_conf):
    """docstring for cmip6_Conf"""

    def __init__(self):

        self._cmip = "cmip6_ng"

        self._files_orig = FileFinder(
            path_pattern="/net/atmos/data/cmip6-ng/{varn}/{timeres}/{grid}/",
            file_pattern="{varn}_{timeres}_{model}_{exp}_{ens}_{grid}.nc",
        )

        self._filefinder_find_all_files_orig_ = self.files_orig.find_files

        self._files_post = FileFinder(
            path_pattern=root_folder_postprocessed_data
            + "cmip6-ng/{varn}/{postprocess}/",
            file_pattern="{postprocess}_{varn}_{model}_{exp}_{ens}_{grid}.nc",
        )

        self._files_fx = FileFinder(
            path_pattern="/net/atmos/data/cmip6-ng/{exp}/{table}/{varn}/{model}/{ens}/{grid}/",
            file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{grid}.nc",
        )

        self._fixes_files = fixes.cmip6_files
        self._fixes_data = fixes.cmip6_data
        self._fixes_preprocess = fixes.cmip6_preprocess

        self._figure_folder = root_folder_figures + "cmip6/cmip6_"
        self.root_folder_figures = root_folder_figures

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

        self.colors = COLORS_SSP


cmip6_ng = _cmip6_ng_conf()


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
