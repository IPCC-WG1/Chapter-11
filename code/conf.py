import numpy as np
from filefinder import FileFinder

# CONFIGURATION FILE

# folder where the postprocessed data is stored
root_folder_postprocessed_data = "../data/"

# =============================================================================
# CMIP Configuration Class
# =============================================================================


class _cmip_conf:
    """docstring for cmip5_Conf"""

    def __init__(self):
        raise ValueError("Use 'conf.cmip5' of 'conf.cmip6' instead")

    @property
    def files_orig(self):
        return self._files_orig

    @property
    def files_post(self):
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


# =============================================================================
# CMIP5 Configuration
# =============================================================================


class _cmip5_conf(_cmip_conf):
    """docstring for cmip5_Conf"""

    def __init__(self):

        self._files_orig = FileFinder(
            path_pattern="/net/atmos/data/cmip5/{exp}/{table}/{varn}/{model}/{ens}/",
            file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{time}.nc",
        )

        self._files_post = FileFinder(
            path_pattern=root_folder_postprocessed_data + "cmip5/{varn}/{postprocess}/",
            file_pattern="{postprocess}_{varn}_{table}_{model}_{exp}_{ens}.nc",
        )

        self._hist_period = slice("1850", "2005")
        self._proj_period = slice("2006", "2099")

        self._scenarios_all = ["rcp26", "rcp45", "rcp60", "rcp85"]
        self._scenarios = ["rcp26", "rcp45", "rcp60", "rcp85"]


cmip5 = _cmip5_conf()

# =============================================================================
# CMIP6 Configuration
# =============================================================================


class _cmip6_conf(_cmip_conf):
    """docstring for cmip6_Conf"""

    def __init__(self):

        self._files_orig = FileFinder(
            path_pattern="/net/atmos/data/cmip6/{exp}/{table}/{varn}/{model}/{ens}/{grid}/",
            file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{grid}_{time}.nc",
        )

        self._files_post = FileFinder(
            path_pattern=root_folder_postprocessed_data + "cmip6/{varn}/{postprocess}/",
            file_pattern="{postprocess}_{varn}_{table}_{model}_{exp}_{ens}_{grid}.nc",
        )

        self._hist_period = slice("1850", "2014")
        self._proj_period = slice("2015", "2099")

        self._scenarios_all = [
            "ssp119",
            "ssp126",
            "ssp245",
            "ssp370",
            "ssp434",
            "ssp460",
            "ssp585",
        ]
        self._scenarios = ["ssp126", "ssp245", "ssp460", "ssp585"]


cmip6 = _cmip6_conf()


# =============================================================================
# ETCCDI - DATA - already prostprocessed
# =============================================================================

"""
postprocessing

1) regridding to 2.5°

    cdo -s -f nc4 -z zip_8 -remapbil,mygrid ${ifile} ${ofile}

    gridtype  = lonlat
    xsize     = 144
    ysize     = 72
    xfirst    = -178.75
    xinc      = 2.5
    yfirst    = -88.75
    yinc      = 2.5

2) create common time axes

    restrict time axes to
        1861 - 2005 for historical
        2006 - 2099 for rcp
        2006 - 2299 for rcp

    cdo -s -f nc4 -z zip_8 -selyear,1861/2005 $fi ${do}/$fo
    cdo -s -f nc4 -z zip_8 -selyear,2006/2099 $fi ${do}/$fo
    cdo -s -f nc4 -z zip_8 -selyear,2006/2299 $fi ${do}/$fo

"""

etccdi_root_folder = (
    "/net/exo/landclim/data/dataset/ETCCDI/20160315/"
    "2.5deg_lat-lon_1y/processed/regrid2.5deg/"
)

# =============================================================================
# Reference Period
# =============================================================================

ANOMALY_YR_START = 1851
ANOMALY_YR_END = 1900

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