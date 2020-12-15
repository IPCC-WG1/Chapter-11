import logging

import docopt
import regionmask

import conf
import fixes
from utils import xarray_utils as xru
from utils.transform import (
    CDD,
    IAV,
    Globmean,
    NoTransform,
    RegionAverage,
    ResampleAnnual,
    ResampleMonthly,
    RollingResampleAnnual,
    SelectGridpoint,
    SelectRegion,
    TX_Days_Above,
    regrid_cdo,
)

logger = logging.getLogger(__name__)

# from dask.distributed import Client
# client = Client()  # set up local cluster
# print(client)


from dask.distributed import Client, LocalCluster

# cluster = LocalCluster(n_workers=1, threads_per_worker=1)


ar6_land = regionmask.defined_regions.ar6.land

# =============================================================================
# define class to postprocess cmip5 and cmip6 data
# =============================================================================


class ProcessCmipDataFromOrig:
    def postprocess_from_orig(
        self, table, varn, postprocess_name, transform_func, exp=None, **kwargs
    ):

        print(f"=== postprocess_from_orig: {postprocess_name} ===\n")

        files = self.conf_cmip.find_all_files_orig(
            table=table, varn=varn, exp=exp, **kwargs
        )

        self.conf_cmip._create_folder_for_output(files, postprocess_name)

        for folder_in, metadata in files:

            metadata["postprocess_name"] = postprocess_name

            fN_out = self.conf_cmip.files_post.create_full_name(
                **metadata, postprocess=postprocess_name
            )

            print(metadata)
            print(folder_in)

            xru.postprocess(
                fN_out,
                metadata=metadata,
                data_reader=self.conf_cmip.load_orig,
                transform_func=transform_func,
            )

    def global_mean_from_orig(
        self, table, varn, postprocess_name="global_mean", exp=None, **kwargs
    ):

        transform_func = Globmean(var=varn)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def no_transform_from_orig(
        self, table, varn, postprocess_name="no_transform", exp=None, **kwargs
    ):

        transform_func = NoTransform(var=varn)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def select_coords_from_orig(
        self, table, varn, postprocess_name, coords, exp=None, **kwargs
    ):

        transform_func = SelectGridpoint(var=varn, **coords)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def select_region_from_orig(
        self, table, varn, postprocess_name, coords, exp=None, **kwargs
    ):

        transform_func = SelectRegion(var=varn, **coords)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def cdd_from_orig(
        self, table, varn="pr", postprocess_name="CDD", freq="A", exp=None, **kwargs
    ):

        transform_func = CDD(var=varn, freq=freq)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def rx5day_from_orig(
        self, table="day", varn="pr", postprocess_name="Rx5day", exp=None, **kwargs
    ):

        transform_func = RollingResampleAnnual(
            var=varn, window=5, how_rolling="sum", how="max"
        )

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def rx30day_from_orig(
        self, table="day", varn="pr", postprocess_name="Rx30day", exp=None, **kwargs
    ):

        transform_func = RollingResampleAnnual(
            var=varn, window=30, how_rolling="sum", how="max"
        )

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def tx_days_above_from_orig(
        self,
        table,
        varn="tasmax",
        postprocess_name="tx_days_above_35",
        thresh="35.0 degC",
        freq="A",
        exp=None,
        **kwargs,
    ):

        transform_func = TX_Days_Above(var=varn, freq=freq, thresh=thresh)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def resample_annual_from_orig(
        self, table, varn, postprocess_name, how, exp=None, **kwargs
    ):

        transform_func = ResampleAnnual(var=varn, how=how)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def resample_monthly_from_orig(
        self, table, varn, postprocess_name, how, exp=None, **kwargs
    ):

        transform_func = ResampleMonthly(var=varn, how=how)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def resample_annual_quantile_from_orig(
        self, table, varn, postprocess_name, q, exp=None, **kwargs
    ):

        transform_func = ResampleAnnual(var=varn, how="quantile", q=q)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def region_average_from_orig(
        self, table, varn, postprocess_name, exp=None, regions=ar6_land, **kwargs
    ):

        transform_func = RegionAverage(varn, regions=regions)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )


class ProcessCmipDataFromPost:
    def regrid_from_post(
        self,
        varn,
        postprocess_before,
        postprocess_name,
        exp="*",
        method="con2",
        **kwargs,
    ):

        print("=== regrid_from_post ===\n")

        files = self.conf_cmip.find_all_files_post(
            postprocess=postprocess_before, varn=varn, exp=exp, **kwargs
        )

        files.df = files.df.drop("postprocess", axis=1)

        self.conf_cmip._create_folder_for_output(files, postprocess_name)

        for fN_in, metadata in files:

            metadata["postprocess_name"] = postprocess_name

            fN_out = self.conf_cmip.files_post.create_full_name(
                **metadata, postprocess=postprocess_name
            )

            print(metadata)
            regrid_cdo(fN_in, fN_out, "g025", method=method)

    def region_average_from_post(
        self,
        varn,
        postprocess_before,
        postprocess_name,
        exp="*",
        regions=ar6_land,
        land_only=True,
        **kwargs,
    ):

        print("=== region_average_from_post ===\n")

        files = self.conf_cmip.find_all_files_post(
            postprocess=postprocess_before, varn=varn, exp=exp, **kwargs
        )

        files.df = files.df.drop("postprocess", axis=1)

        self.conf_cmip._create_folder_for_output(files, postprocess_name)

        transform_func = RegionAverage(varn, regions=regions, land_only=land_only)

        for fN_in, metadata in files:

            metadata["postprocess_name"] = postprocess_name
            metadata["postprocess"] = postprocess_before

            fN_out = self.conf_cmip.files_post.create_full_name(**metadata)

            print(metadata)
            xru.postprocess(
                fN_out,
                metadata,
                data_reader=self.conf_cmip.load_postprocessed,
                transform_func=transform_func,
            )

    def iav_from_post(
        self,
        varn,
        postprocess_before,
        postprocess_name,
        exp="piControl",
        period=20,
        min_length=500,
        cut_start=100,
        deg=2,
        **kwargs,
    ):

        print("=== iav_average_from_post ===\n")

        files = self.conf_cmip.find_all_files_post(
            postprocess=postprocess_before, varn=varn, exp=exp, **kwargs
        )

        files.df = files.df.drop("postprocess", axis=1)

        self.conf_cmip._create_folder_for_output(files, postprocess_name)

        transform_func = IAV(
            varn, period=period, min_length=min_length, cut_start=cut_start, deg=deg
        )

        for fN_in, metadata in files:

            fN_out = self.conf_cmip.files_post.create_full_name(
                postprocess=postprocess_name, **metadata
            )

            metadata["postprocess_name"] = postprocess_name
            metadata["postprocess"] = postprocess_before

            print(metadata)
            xru.postprocess(
                fN_out,
                metadata,
                data_reader=self.conf_cmip.load_postprocessed,
                transform_func=transform_func,
            )


class ProcessCmipData(ProcessCmipDataFromOrig, ProcessCmipDataFromPost):
    def __init__(self, conf_cmip):

        self.conf_cmip = conf_cmip
        self.fixes_data = conf_cmip.fixes_data
        self.fixes_files = conf_cmip.fixes_files
        self.fixes_common = fixes.fixes_common


# =============================================================================
# instantiate the postprocess class
# =============================================================================


process_cmip5_data = ProcessCmipData(conf.cmip5)
process_cmip6_data = ProcessCmipData(conf.cmip6)


# =============================================================================
# calculate global mean tas
# =============================================================================


def tas_globmean():

    process_cmip5_data.global_mean_from_orig(
        table="Amon",
        varn="tas",
        postprocess_name="global_mean",
        exp=conf.cmip5.scenarios_all_incl_hist,
        ensnumber=None,
    )

    process_cmip6_data.global_mean_from_orig(
        table="Amon",
        varn="tas",
        postprocess_name="global_mean",
        exp=conf.cmip6.scenarios_all_incl_hist,
        ensnumber=None,
    )


def tas_annmean():

    process_cmip6_data.resample_annual_from_orig(
        table="Amon", varn="tas", postprocess_name="annmean", how="mean", exp=None
    )

    process_cmip6_data.resample_annual_from_orig(
        table="Amon",
        varn="tas",
        postprocess_name="annmean",
        how="mean",
        exp="piControl",
    )

    process_cmip6_data.regrid_from_post(
        varn="tas",
        postprocess_before="annmean",
        postprocess_name="annmean_regrid",
        exp="*",
    )

    process_cmip6_data.iav_from_post(
        varn="tas",
        postprocess_before="annmean_regrid",
        postprocess_name="annmean_regrid_IAV20",
        exp="piControl",
    )

    process_cmip6_data.iav_from_post(
        varn="tas",
        postprocess_before="annmean_regrid",
        postprocess_name="annmean_regrid_IAV1",
        exp="piControl",
        period=1,
    )

    process_cmip6_data.region_average_from_post(
        varn="tas",
        postprocess_before="annmean",
        postprocess_name="annmean_reg_ave_ar6",
        exp="*",
    )


def tas_monthly():

    process_cmip6_data.resample_monthly_from_orig(
        table="Amon", varn="tas", postprocess_name="monthly", how="mean", exp=None
    )

    process_cmip6_data.regrid_from_post(
        varn="tas",
        postprocess_before="monthly",
        postprocess_name="monthly_regrid",
        exp=None,
    )


#     process_cmip6_data.region_average_from_post(
#         varn="tas",
#         postprocess_before="monthly",
#         postprocess_name="monthly_reg_ave_ar6",
#         exp="*",
#     )


def pr_annmean():

    process_cmip6_data.resample_annual_from_orig(
        table="Amon", varn="pr", postprocess_name="annmean", how="mean", exp=None
    )

    process_cmip6_data.resample_annual_from_orig(
        table="Amon", varn="pr", postprocess_name="annmean", how="mean", exp="piControl"
    )
    # regrid
    # ======

    process_cmip6_data.regrid_from_post(
        varn="pr",
        postprocess_before="annmean",
        postprocess_name="annmean_regrid",
        exp="*",
    )

    # IAV
    # ===

    process_cmip6_data.iav_from_post(
        varn="pr",
        postprocess_before="annmean_regrid",
        postprocess_name="annmean_regrid_IAV20",
        exp="piControl",
    )

    process_cmip6_data.iav_from_post(
        varn="pr",
        postprocess_before="annmean_regrid",
        postprocess_name="annmean_regrid_IAV1",
        exp="piControl",
        period=1,
    )

    # region average
    # ==============


def pr_monthly():

    process_cmip6_data.resample_monthly_from_orig(
        table="Amon", varn="pr", postprocess_name="monthly", how="mean", exp=None
    )

    process_cmip6_data.regrid_from_post(
        varn="pr",
        postprocess_before="monthly",
        postprocess_name="monthly_regrid",
        exp=None,
    )


#     process_cmip6_data.region_average_from_post(
#         varn="pr",
#         postprocess_before="monthly",
#         postprocess_name="monthly_reg_ave_ar6",
#         exp="*",
#     )

# # =============================================================================
# # calculate txx
# # =============================================================================


def txx():

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="tasmax", postprocess_name="txx", how="max", exp=None
    )

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="tasmax", postprocess_name="txx", how="max", exp="piControl"
    )

    # regrid
    # ======

    process_cmip6_data.regrid_from_post(
        varn="tasmax",
        postprocess_before="txx",
        postprocess_name="txx_regrid",
        exp="*",
        # ensnumber=None,
    )

    # IAV
    # ===

    process_cmip6_data.iav_from_post(
        varn="tasmax",
        postprocess_before="txx_regrid",
        postprocess_name="txx_regrid_IAV20",
        exp="piControl",
    )

    process_cmip6_data.iav_from_post(
        varn="tasmax",
        postprocess_before="txx_regrid",
        postprocess_name="txx_regrid_IAV1",
        exp="piControl",
        period=1,
    )

    # region average
    # ==============

    process_cmip6_data.region_average_from_post(
        varn="tasmax",
        postprocess_before="txx",
        postprocess_name="txx_reg_ave_ar6",
        exp="*",
    )


def txx_monthly():

    # monthly maximum temperature
    process_cmip6_data.resample_monthly_from_orig(
        table="day", varn="tasmax", postprocess_name="txx_monthly", how="max", exp=None
    )

    process_cmip6_data.regrid_from_post(
        varn="tasmax",
        postprocess_before="txx_monthly",
        postprocess_name="txx_monthly_regrid",
        exp="*",
        # ensnumber=None,
    )


# # =============================================================================
# # calculate > 35° C
# # =============================================================================


def tx_days_above():

    process_cmip6_data.tx_days_above_from_orig(
        table="day",
        varn="tasmax",
        postprocess_name="tx_days_above_35",
        thresh="35.0 degC",
        freq="A",
        exp=None,
    )

    process_cmip6_data.regrid_from_post(
        varn="tasmax",
        postprocess_before="tx_days_above_35",
        postprocess_name="tx_days_above_35_regrid",
        exp=None,
    )


# # =============================================================================
# # calculate txp95
# # =============================================================================


def txp95():

    # process_cmip5_data.resample_annual_quantile_from_orig(
    #     table="day", varn="tasmax", postprocess_name="txp95", q=0.95, exp=None
    # )

    process_cmip6_data.resample_annual_quantile_from_orig(
        table="day", varn="tasmax", postprocess_name="txp95", q=0.95, exp=None
    )

    # process_cmip5_data.resample_annual_quantile_from_orig(
    #     table="day", varn="tasmax", postprocess_name="txp95", q=0.95, exp="piControl"
    # )
    #
    # process_cmip6_data.resample_annual_quantile_from_orig(
    #     table="day", varn="tasmax", postprocess_name="txp95", q=0.95, exp="piControl"
    # )

    # # regrid txp95
    # # =============================================================================

    # process_cmip5_data.regrid_from_post(
    #     varn="tasmax",
    #     postprocess_before="txp95",
    #     postprocess_name="txp95_regrid",
    #     exp="*",
    # )
    #
    # process_cmip6_data.regrid_from_post(
    #     varn="tasmax",
    #     postprocess_before="txp95",
    #     postprocess_name="txp95_regrid",
    #     exp="*",
    # )

    # # region average txp95
    # # =============================================================================

    # process_cmip5_data.region_average_from_post(
    #     varn="tasmax",
    #     postprocess_before="txp95",
    #     postprocess_name="txp95_reg_ave_ar6",
    #     exp="*",
    # )
    # process_cmip6_data.region_average_from_post(
    #     varn="tasmax",
    #     postprocess_before="txp95",
    #     postprocess_name="txp95_reg_ave_ar6",
    #     exp="*",
    # )


# # =============================================================================
# # calculate tnn
# # =============================================================================


def tnn():

    transform_func = ResampleAnnual(var="tasmin", how="min")

    # process_cmip5_data.postprocess_from_orig(
    #     table="day",
    #     varn="tasmin",
    #     postprocess_name="tnn",
    #     transform_func=transform_func,
    #     exp=None,
    # )

    process_cmip6_data.postprocess_from_orig(
        table="day",
        varn="tasmin",
        postprocess_name="tnn",
        transform_func=transform_func,
        exp=None,
    )

    # process_cmip5_data.postprocess_from_orig(
    #     table="day",
    #     varn="tasmin",
    #     postprocess_name="tnn",
    #     transform_func=transform_func,
    #     exp="piControl",
    # )

    process_cmip6_data.postprocess_from_orig(
        table="day",
        varn="tasmin",
        postprocess_name="tnn",
        transform_func=transform_func,
        exp="piControl",
    )

    # # regrid tnn
    # # =============================================================================

    # process_cmip5_data.regrid_from_post(
    #     varn="tasmin", postprocess_before="tnn", postprocess_name="tnn_regrid", exp="*"
    # )

    process_cmip6_data.regrid_from_post(
        varn="tasmin", postprocess_before="tnn", postprocess_name="tnn_regrid", exp="*"
    )

    # # region average tnn
    # # =============================================================================

    # process_cmip5_data.region_average_from_post(
    #     varn="tasmin",
    #     postprocess_before="tnn",
    #     postprocess_name="tnn_reg_ave_ar6",
    #     exp="*",
    # )
    process_cmip6_data.region_average_from_post(
        varn="tasmin",
        postprocess_before="tnn",
        postprocess_name="tnn_reg_ave_ar6",
        exp="*",
    )


def tnn_monthly():

    # monthly maximum temperature
    process_cmip6_data.resample_monthly_from_orig(
        table="day", varn="tasmin", postprocess_name="tnn_monthly", how="min", exp=None
    )

    process_cmip6_data.regrid_from_post(
        varn="tasmin",
        postprocess_before="tnn_monthly",
        postprocess_name="tnn_monthly_regrid",
        exp="*",
        # ensnumber=None,
    )


# =============================================================================
# calculate rx1day
# =============================================================================


def rx1day():

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="pr", postprocess_name="rx1day", how="max", exp=None
    )

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="pr", postprocess_name="rx1day", how="max", exp="piControl"
    )

    # regrid
    # ======

    process_cmip6_data.regrid_from_post(
        varn="pr",
        postprocess_before="rx1day",
        postprocess_name="rx1day_regrid",
        exp="*",
    )

    # IAV
    # ===

    process_cmip6_data.iav_from_post(
        varn="pr",
        postprocess_before="rx1day_regrid",
        postprocess_name="rx1day_regrid_IAV20",
        exp="piControl",
    )

    process_cmip6_data.iav_from_post(
        varn="pr",
        postprocess_before="rx1day_regrid",
        postprocess_name="rx1day_regrid_IAV1",
        exp="piControl",
        period=1,
    )

    # region average
    # ==============

    process_cmip6_data.region_average_from_post(
        varn="pr",
        postprocess_before="rx1day",
        postprocess_name="rx1day_reg_ave_ar6",
        exp="*",
    )


def rx1day_monthly():

    process_cmip6_data.resample_monthly_from_orig(
        table="day", varn="pr", postprocess_name="rx1day_monthly", how="max", exp=None
    )

    process_cmip6_data.regrid_from_post(
        varn="pr",
        postprocess_before="rx1day_monthly",
        postprocess_name="rx1day_monthly_regrid",
        exp="*",
    )


# =============================================================================
# calculate rx5day
# =============================================================================


def rx5day():

    process_cmip6_data.rx5day_from_orig(
        table="day", varn="pr", postprocess_name="rx5day", exp=None
    )

    process_cmip6_data.rx5day_from_orig(
        table="day", varn="pr", postprocess_name="rx5day", exp="piControl"
    )

    # regrid
    # ======

    process_cmip6_data.regrid_from_post(
        varn="pr",
        postprocess_before="rx5day",
        postprocess_name="rx5day_regrid",
        exp="*",
    )

    # IAV
    # ===

    process_cmip6_data.iav_from_post(
        varn="pr",
        postprocess_before="rx5day_regrid",
        postprocess_name="rx5day_regrid_IAV20",
        exp="piControl",
    )

    process_cmip6_data.iav_from_post(
        varn="pr",
        postprocess_before="rx5day_regrid",
        postprocess_name="rx5day_regrid_IAV1",
        exp="piControl",
        period=1,
    )

    # region average
    # ==============

    process_cmip6_data.region_average_from_post(
        varn="pr",
        postprocess_before="rx5day",
        postprocess_name="rx5day_reg_ave_ar6",
        exp="*",
    )


# =============================================================================
# calculate rx30day
# =============================================================================


def rx30day():

    process_cmip6_data.rx30day_from_orig(
        table="day", varn="pr", postprocess_name="rx30day", exp=None
    )

    process_cmip6_data.rx30day_from_orig(
        table="day", varn="pr", postprocess_name="rx30day", exp="piControl"
    )
    process_cmip6_data.regrid_from_post(
        varn="pr",
        postprocess_before="rx30day",
        postprocess_name="rx30day_regrid",
        exp="*",
    )
    process_cmip6_data.region_average_from_post(
        varn="pr",
        postprocess_before="rx30day",
        postprocess_name="rx30day_reg_ave_ar6",
        exp="*",
    )


# =============================================================================
# calculate cdd
# =============================================================================


def cdd():

    #     process_cmip5_data.cdd_from_orig(
    #         table="day", varn="pr", postprocess_name="cdd", exp=None
    #     )

    process_cmip6_data.cdd_from_orig(
        table="day", varn="pr", postprocess_name="cdd", exp=None
    )

    # process_cmip5_data.cdd_from_orig(
    #    table="day", varn="pr", postprocess_name="cdd", exp="piControl"
    # )

    # process_cmip6_data.cdd_from_orig(
    #     table="day", varn="pr", postprocess_name="cdd", exp="piControl"
    # )

    # regrid cdd
    # =============================================================================

    #     process_cmip5_data.regrid_from_post(
    #         varn="pr", postprocess_before="cdd", postprocess_name="cdd_regrid", exp="*"
    #     )
    process_cmip6_data.regrid_from_post(
        varn="pr", postprocess_before="cdd", postprocess_name="cdd_regrid", exp="*"
    )

    # region average cdd
    # =============================================================================

    #     process_cmip5_data.region_average_from_post(
    #         varn="pr", postprocess_before="cdd", postprocess_name="cdd_reg_ave_ar6", exp="*"
    #     )
    process_cmip6_data.region_average_from_post(
        varn="pr", postprocess_before="cdd", postprocess_name="cdd_reg_ave_ar6", exp="*"
    )


# =============================================================================
# calculate mrso
# =============================================================================


def mrso():

    #     process_cmip5_data.no_transform_from_orig(
    #         table="Lmon", varn="mrso", postprocess_name="sm", exp=None
    #     )

    process_cmip6_data.no_transform_from_orig(
        table="Lmon", varn="mrso", postprocess_name="sm", exp=None
    )

    # process_cmip5_data.no_transform_from_orig(
    #    table="Lmon", varn="mrso", postprocess_name="sm", exp="piControl"
    # )

    # process_cmip6_data.no_transform_from_orig(
    #    table="Lmon", varn="mrso", postprocess_name="sm", exp="piControl"
    # )

    # regrid sm
    # =============================================================================

    #     process_cmip5_data.regrid_from_post(
    #         varn="mrso", postprocess_before="sm", postprocess_name="sm_regrid", exp="*"
    #     )
    process_cmip6_data.regrid_from_post(
        varn="mrso", postprocess_before="sm", postprocess_name="sm_regrid", exp="*"
    )

    # region average sm
    # =============================================================================

    #     process_cmip5_data.region_average_from_post(
    #         varn="mrso", postprocess_before="sm", postprocess_name="sm_reg_ave_ar6", exp="*"
    #     )
    process_cmip6_data.region_average_from_post(
        varn="mrso", postprocess_before="sm", postprocess_name="sm_reg_ave_ar6", exp="*"
    )


def mrso_annmean():

    process_cmip6_data.resample_annual_from_orig(
        table="Lmon", varn="mrso", postprocess_name="sm_annmean", how="mean", exp=None
    )

    # regrid sm
    # =============================================================================

    process_cmip6_data.regrid_from_post(
        varn="mrso",
        postprocess_before="sm_annmean",
        postprocess_name="sm_annmean_regrid",
        exp="*",
    )

    # region average sm
    # =============================================================================

    process_cmip6_data.region_average_from_post(
        varn="mrso",
        postprocess_before="sm_annmean",
        postprocess_name="sm_annmean_reg_ave_ar6",
        exp="*",
    )


# =============================================================================
# calculate mrsos
# =============================================================================


def mrsos():

    #     process_cmip5_data.no_transform_from_orig(
    #         table="Lmon", varn="mrsos", postprocess_name="sm", exp=None
    #     )

    process_cmip6_data.no_transform_from_orig(
        table="Lmon", varn="mrsos", postprocess_name="sm", exp=None
    )

    # process_cmip5_data.no_transform_from_orig(
    #    table="Lmon", varn="mrsos", postprocess_name="sm", exp="piControl"
    # )

    # process_cmip6_data.no_transform_from_orig(
    #    table="Lmon", varn="mrsos", postprocess_name="sm", exp="piControl"
    # )

    # regrid sm
    # =============================================================================

    #     process_cmip5_data.regrid_from_post(
    #         varn="mrsos", postprocess_before="sm", postprocess_name="sm_regrid", exp="*"
    #     )
    process_cmip6_data.regrid_from_post(
        varn="mrsos", postprocess_before="sm", postprocess_name="sm_regrid", exp="*"
    )

    # region average sm
    # =============================================================================

    #     process_cmip5_data.region_average_from_post(
    #         varn="mrsos",
    #         postprocess_before="sm",
    #         postprocess_name="sm_reg_ave_ar6",
    #         exp="*",
    #     )
    process_cmip6_data.region_average_from_post(
        varn="mrsos",
        postprocess_before="sm",
        postprocess_name="sm_reg_ave_ar6",
        exp="*",
    )


def mrsos_annmean():

    process_cmip6_data.resample_annual_from_orig(
        table="Lmon", varn="mrsos", postprocess_name="sm_annmean", how="mean", exp=None
    )

    # regrid sm
    # =============================================================================

    process_cmip6_data.regrid_from_post(
        varn="mrsos",
        postprocess_before="sm_annmean",
        postprocess_name="sm_annmean_regrid",
        exp="*",
    )

    # region average sm
    # =============================================================================

    process_cmip6_data.region_average_from_post(
        varn="mrsos",
        postprocess_before="sm_annmean",
        postprocess_name="sm_annmean_reg_ave_ar6",
        exp="*",
    )


def seaice_any_annual():

    process_cmip6_data.resample_annual_from_orig(
        table="SImon", varn="siconc", postprocess_name="any_annual", how="any", exp=None
    )

    # regrid
    # =============================================================================

    # region average
    # =============================================================================


def region_average_arctic_mid_lat():
    # for SPM Sonia & Ed
    from utils import regions

    mid_lat_arctic_region = regions.mid_lat_arctic_region

    process_cmip6_data.region_average_from_post(
        varn="tasmax",
        postprocess_before="txx",
        postprocess_name="txx_reg_ave_mid_lat_arctic",
        exp="*",
        regions=mid_lat_arctic_region,
        land_only=False,
    )

    process_cmip6_data.region_average_from_post(
        varn="tasmax",
        postprocess_before="txx_monthly",
        postprocess_name="txx_monthly_reg_ave_mid_lat_arctic",
        exp="*",
        regions=mid_lat_arctic_region,
        land_only=False,
    )

    process_cmip6_data.region_average_from_post(
        varn="tasmin",
        postprocess_before="tnn",
        postprocess_name="tnn_reg_ave_mid_lat_arctic",
        exp="*",
        regions=mid_lat_arctic_region,
        land_only=False,
    )

    process_cmip6_data.region_average_from_post(
        varn="tasmin",
        postprocess_before="tnn_monthly",
        postprocess_name="tnn_monthly_reg_ave_mid_lat_arctic",
        exp="*",
        regions=mid_lat_arctic_region,
        land_only=False,
    )


# =============================================================================
# main
# =============================================================================


def main(args=None):
    """
    process.py
    Usage:
      process.py <postprocess>
      process.py -h | --help

    Examples:
      process.py txx
    """

    client = Client()
    print(client)

    # parse cmd line arguments
    options = docopt.docopt(main.__doc__, version=None)

    postprocess = options["<postprocess>"]

    functions = {
        "tas_globmean": tas_globmean,
        "tas_annmean": tas_annmean,
        "tas_monthly": tas_monthly,
        "pr_annmean": pr_annmean,
        "pr_monthly": pr_monthly,
        "txx": txx,
        "txx_monthly": txx_monthly,
        "tx_days_above": tx_days_above,
        "txp95": txp95,
        "tnn": tnn,
        "tnn_monthly": tnn_monthly,
        "rx1day": rx1day,
        "rx1day_monthly": rx1day_monthly,
        "rx5day": rx5day,
        "rx30day": rx30day,
        "cdd": cdd,
        "mrso": mrso,
        "mrso_annmean": mrso_annmean,
        "mrsos": mrsos,
        "mrsos_annmean": mrsos_annmean,
        "seaice_any_annual": seaice_any_annual,
        "region_average_arctic_mid_lat": region_average_arctic_mid_lat,
    }

    func = functions[postprocess]

    func()


if __name__ == "__main__":
    main()
