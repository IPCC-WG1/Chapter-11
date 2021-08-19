import logging

import docopt
import regionmask

import conf
from utils import postprocess

logger = logging.getLogger(__name__)

ar6_land = regionmask.defined_regions.ar6.land

# =============================================================================


# postprocess.GlobalMeanFromOrig(self.conf_cmip)
# postprocess.NoTransformFromOrig(self.conf_cmip)
# postprocess.SelectGridpointFromOrig(self.conf_cmip)
# postprocess.SelectRegionFromOrig(self.conf_cmip)
# postprocess.CDDFromOrig(self.conf_cmip)
# postprocess.RxNdayFromOrig(self.conf_cmip)
# postprocess.TxDaysAboveFromOrig(self.conf_cmip)
# postprocess.ResampleAnnualFromOrig(self.conf_cmip)
# postprocess.ResampleMonthlyFromOrig(self.conf_cmip)
# postprocess.ResampleAnnualQuantileFromOrig(self.conf_cmip)
# postprocess.RegionAverageFromOrig(self.conf_cmip)

# postprocess.RegridFromPost(self.conf_cmip)
# postprocess.RegionAverageFromPost(self.conf_cmip)
# postprocess.IAVFromPost(self.conf_cmip)

# =============================================================================

GREENLAND = regionmask.defined_regions.natural_earth.countries_110[["Greenland"]]

# =============================================================================
# helper classes
# =============================================================================


class RegridFromPostMixin:
    def regrid_from_post(self, target_grid="g025", method="con2"):

        grid = "" if target_grid == "g025" else f"_{target_grid}"

        with postprocess.RegridFromPost(self.conf_cmip) as p:
            p.postprocess_name = f"{self.postprocess_name}_regrid{grid}"
            p.set_files_kwargs(
                table=self.table,
                varn=self.varn,
                exp="*",
                postprocess=self.postprocess_name,
            )
            p.transform(target_grid=target_grid, method=method)


class RegionAverageFromPostMixin:
    def region_average_from_post(
        self,
        lat_weights,
        weights,
        region=ar6_land,
        region_name="ar6",
        ensnumber=0,
    ):

        with postprocess.RegionAverageFromPost(self.conf_cmip) as p:
            p.postprocess_name = f"{self.postprocess_name}_reg_ave_{region_name}"
            p.set_files_kwargs(
                table=self.table,
                varn=self.varn,
                exp="*",
                postprocess=self.postprocess_name,
                ensnumber=ensnumber,
            )
            p.transform(regions=region, lat_weights=lat_weights, weights=weights)


class ResampleSeasonalFromPostMixin:
    def resample_seasonal_from_post(
        self, how, invalidate_beg_end, from_concat, after_regrid
    ):

        name_cat = "cat" if from_concat else ""
        name_regrid = "_regrid" if after_regrid else ""
        postprocess_name = f"{self.postprocess_name}{name_regrid}"

        exp = "*"

        # exclude "historical"
        if exp == "*" and from_concat:
            exp = self.conf_cmip.scenarios_all
        elif from_concat and "historical" in exp:
            exp.remove("historical")

        with postprocess.ResampleSeasonal(self.conf_cmip) as p:
            p.postprocess_name = f"{postprocess_name}_seas_{how}_{name_cat}"
            p.set_files_kwargs(
                table=self.table,
                varn=self.varn,
                exp=exp,
                postprocess=postprocess_name,
            )
            p.transform(how, invalidate_beg_end, from_concat)


class IAV20FromPostMixin:
    def iav20_after_regrid_from_post(self):

        with postprocess.IAVFromPost(self.conf_cmip) as p:
            p.postprocess_name = f"{self.postprocess_name}_regrid_IAV20"
            p.set_files_kwargs(
                table=self.table,
                varn=self.varn,
                exp="piControl",
                postprocess=f"{self.postprocess_name}_regrid",
            )
            p.transform()


# =============================================================================
# helper classes
# =============================================================================


class ResampleAnnual(
    RegridFromPostMixin, RegionAverageFromPostMixin, IAV20FromPostMixin
):
    def __init__(self, conf_cmip, table, varn, how, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.how = how
        self.postprocess_name = postprocess_name

    def annual_from_orig(self, exp=None, mask_out=None):

        with postprocess.ResampleAnnualFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform(how=self.how, mask_out=mask_out)

    def annual_from_orig_pi_control(self, mask_out=None):
        self.annual_from_orig(exp="piControl", mask_out=mask_out)


class ResampleMonthly(
    RegridFromPostMixin, RegionAverageFromPostMixin, ResampleSeasonalFromPostMixin
):
    def __init__(self, conf_cmip, table, varn, how, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.how = how
        self.postprocess_name = postprocess_name

    def resample_monthly_from_orig(self, mask_out=None):
        with postprocess.ResampleMonthlyFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=None)
            p.transform(how=self.how)


class NoTransform(
    RegridFromPostMixin, RegionAverageFromPostMixin, ResampleSeasonalFromPostMixin
):
    def __init__(self, conf_cmip, table, varn, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.postprocess_name = postprocess_name

    def no_transform_from_orig(self, mask_out=None):
        with postprocess.NoTransformFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=None)
            p.transform(mask_out=mask_out)


class SelectRegion(RegridFromPostMixin, RegionAverageFromPostMixin):
    """transformation function to subset a square region"""

    def __init__(
        self, conf_cmip, table, varn, postprocess_name, exp=None, ensnumber=0, **coords
    ):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.postprocess_name = postprocess_name
        self.exp = exp
        self.ensnumber = ensnumber
        self.coords = coords

    def select_region_from_orig(self, mask_out=None):

        with postprocess.SelectRegionFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(
                table=self.table, varn=self.varn, exp=self.exp, ensnumber=self.ensnumber
            )
            p.transform(coords=self.coords, mask_out=mask_out)


class RxNday(RegridFromPostMixin, RegionAverageFromPostMixin, IAV20FromPostMixin):
    def __init__(self, conf_cmip, window):

        self.conf_cmip = conf_cmip
        self.table = "day"
        self.varn = "pr"
        self.window = window
        self.postprocess_name = f"rx{window}day"

    def rxnday_from_orig(self, exp=None):
        with postprocess.RxNdayFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform(window=self.window)

    def rxnday_from_orig_pi_control(self):
        self.rxnday_from_orig(exp="piControl")


class CDD(RegridFromPostMixin, RegionAverageFromPostMixin, IAV20FromPostMixin):
    def __init__(self, conf_cmip, freq="A"):

        self.conf_cmip = conf_cmip
        self.table = "day"
        self.varn = "pr"
        self.freq = freq
        self.postprocess_name = "cdd"

    def cdd_from_orig(self, exp=None):
        with postprocess.CDDFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform(freq=self.freq)

    def cdd_from_orig_pi_control(self):
        self.cdd_from_orig(exp="piControl")


class TxDaysAboveFromOrig(RegridFromPostMixin, RegionAverageFromPostMixin):
    def __init__(self, conf_cmip, thresh, freq, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = "day"
        self.varn = "tasmax"
        self.thresh = thresh
        self.freq = freq
        self.postprocess_name = postprocess_name

    def tx_days_above_from_orig(self, exp=None):
        with postprocess.ResampleAnnualFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform()

    def tx_days_above_from_orig_pi_control(self):
        self.tx_days_above_from_orig(exp="piControl")


class ResampleAnnualQuantileFromOrig(RegridFromPostMixin, RegionAverageFromPostMixin):
    def __init__(self, conf_cmip, table, varn, q, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.q = q
        self.postprocess_name = postprocess_name

    def resample_annual_quantile_from_orig(self, exp=None, mask_out=None):
        with postprocess.ResampleAnnualQuantileFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform(q=self.q, mask_out=mask_out)

    def resample_annual_quantile_from_orig_pi_control(self, mask_out=None):
        self.resample_annual_quantile_from_orig(exp="piControl", mask_out=mask_out)


class ConsecutiveMonthsClimFromOrig(RegridFromPostMixin):
    def __init__(self, conf_cmip, table, varn, how, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.how = how
        self.postprocess_name = postprocess_name

    def consecutive_max_min_months(
        self, beg=1850, end=1900, exp="historical", mask_out=None
    ):

        with postprocess.ConsecutiveMonthsClim(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform(how=self.how, beg=beg, end=end, dim="time", mask_out=mask_out)


class SMDryDaysZhangFromOrig(
    RegridFromPostMixin, RegionAverageFromPostMixin, IAV20FromPostMixin
):
    def __init__(self, conf_cmip, table, varn, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.quantile = 0.1
        self.postprocess_name = postprocess_name
        self.postprocess_name_clim = postprocess_name + "_clim"

    def sm_dry_days_clim_from_orig(self, mask_out=None):

        with postprocess.SMDryDaysClimZhangFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name_clim
            p.set_files_kwargs(table=self.table, varn=self.varn, exp="historical")
            p.transform(quantile=self.quantile, mask_out=mask_out)

    def sm_dry_days_from_orig(self, exp=None, mask_out=None):

        with postprocess.SMDryDaysZhangFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform(self.postprocess_name_clim, is_pic=False, mask_out=mask_out)

    def sm_dry_days_from_orig_pi_control(self, mask_out=None):

        with postprocess.SMDryDaysZhangFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp="piControl")
            p.transform(self.postprocess_name_clim, is_pic=True, mask_out=mask_out)


class SMDryDaysIntensityZhangFromOrig(
    RegridFromPostMixin, RegionAverageFromPostMixin, IAV20FromPostMixin
):
    def __init__(self, conf_cmip, table, varn, postprocess_name):

        self.conf_cmip = conf_cmip
        self.table = table
        self.varn = varn
        self.quantile = 0.1
        self.postprocess_name = postprocess_name + "_intensity"
        self.postprocess_name_clim = postprocess_name + "_clim"

    # get the climatology from SMDryDaysZhangFromOrig

    def sm_dry_days_intensity_from_orig(self, exp=None, mask_out=None):

        with postprocess.SMDryDaysIntensityZhangFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp=exp)
            p.transform(self.postprocess_name_clim, is_pic=False, mask_out=mask_out)

    def sm_dry_days_intensity_from_orig_pi_control(self, mask_out=None):

        with postprocess.SMDryDaysIntensityZhangFromOrig(self.conf_cmip) as p:
            p.postprocess_name = self.postprocess_name
            p.set_files_kwargs(table=self.table, varn=self.varn, exp="piControl")
            p.transform(self.postprocess_name_clim, is_pic=True, mask_out=mask_out)


# =============================================================================
# tas
# =============================================================================


def tas_globmean():

    with postprocess.GlobalMeanFromOrig(conf.cmip5) as p:
        p.postprocess_name = "global_mean"
        p.set_files_kwargs(
            table="Amon",
            varn="tas",
            exp=conf.cmip5.scenarios_all_incl_hist,
            ensnumber=None,
        )
        p.transform(lat_weights="areacella")

    with postprocess.GlobalMeanFromOrig(conf.cmip6) as p:
        p.postprocess_name = "global_mean"
        p.set_files_kwargs(
            table="Amon",
            varn="tas",
            exp=conf.cmip6.scenarios_all_incl_hist,
            ensnumber=None,
        )
        p.transform(lat_weights="areacella")


def tos_globmean():

    with postprocess.GlobalMeanFromOrig(conf.cmip6) as p:
        p.postprocess_name = "global_mean"
        p.set_files_kwargs(
            table="Omon",
            varn="tos",
        )
        p.transform(lat_weights="areacello")

    with postprocess.GlobalMeanFromOrig(conf.cmip5) as p:
        p.postprocess_name = "global_mean"
        p.set_files_kwargs(
            table="Omon",
            varn="tos",
        )
        p.transform(lat_weights="areacello")

    with postprocess.TosGlobmeanMaskIceAnyFromOrig(conf.cmip5) as p:
        p.postprocess_name = "global_mean_masked_ice_any"
        p.set_files_kwargs(
            table="Omon",
            varn="tos",
        )
        p.transform(lat_weights="areacello")

    with postprocess.TosGlobmeanMaskIceAnyFromOrig(conf.cmip6) as p:
        p.postprocess_name = "global_mean_masked_ice_any"
        p.set_files_kwargs(
            table="Omon",
            varn="tos",
        )
        p.transform(lat_weights="areacello")


def tas_annmean():

    p_ = ResampleAnnual(conf.cmip6, "Amon", "tas", "mean", "annmean")
    p_.annual_from_orig()
    p_.annual_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


def tas_monthly():

    p_ = ResampleMonthly(conf.cmip6, "Amon", "tas", "mean", "monthly")
    p_.resample_monthly_from_orig()
    p_.regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")
    p_.resample_seasonal_from_post(
        "mean", invalidate_beg_end=True, from_concat=True, after_regrid=True
    )


def tas_summer_months():

    p_ = ConsecutiveMonthsClimFromOrig(
        conf.cmip6,
        table="Amon",
        varn="tas",
        how="max",
        postprocess_name="summer_months",
    )

    p_.consecutive_max_min_months(beg=1850, end=1900, exp="historical")

    # only for illustration purposes, use largest area fraction remapping
    p_.regrid_from_post(method="laf")


# =============================================================================
# pr
# =============================================================================


def pr_annmean():

    p_ = ResampleAnnual(conf.cmip6, "Amon", "pr", "mean", "annmean")
    p_.annual_from_orig()
    p_.annual_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


def pr_monthly():

    p_ = ResampleMonthly(conf.cmip6, "Amon", "pr", "mean", "monthly")
    p_.resample_monthly_from_orig()
    p_.regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")
    p_.resample_seasonal_from_post(
        "mean", invalidate_beg_end=True, from_concat=True, after_regrid=True
    )


# # =============================================================================
# # calculate txx
# # =============================================================================


def txx():

    p_ = ResampleAnnual(conf.cmip6, "day", "tasmax", "max", "txx")
    p_.annual_from_orig()
    p_.annual_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


def txx_monthly():

    p_ = ResampleMonthly(conf.cmip6, "day", "tasmax", "max", "txx_monthly")
    p_.resample_monthly_from_orig()
    p_.regrid_from_post()
    # p_.region_average_from_post(lat_weights="areacella", weights="land")
    p_.resample_seasonal_from_post(
        "mean", invalidate_beg_end=True, from_concat=True, after_regrid=True
    )


# # =============================================================================
# # calculate > 35° C
# # =============================================================================


def tx_days_above():

    p_ = TxDaysAboveFromOrig(
        conf.cmip6, "35.0 degC", freq="A", postprocess_name="tx_days_above_35"
    )
    p_.tx_days_above_from_orig()
    p_.regrid_from_post()


# # =============================================================================
# # calculate txp95
# # =============================================================================


def txp95():

    p_ = ResampleAnnualQuantileFromOrig(
        conf.cmip6, "day", "tasmay", q=0.95, postprocess_name="txp95"
    )
    p_.resample_annual_quantile_from_orig()
    p_.resample_annual_quantile_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


# # =============================================================================
# # calculate tnn
# # =============================================================================


def tnn():

    p_ = ResampleAnnual(conf.cmip6, "day", "tasmin", "min", "tnn")
    p_.annual_from_orig()
    p_.annual_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


def tnn_monthly():

    p_ = ResampleMonthly(conf.cmip6, "day", "tasmin", "min", "tnn_monthly")
    p_.resample_monthly_from_orig()
    p_.regrid_from_post()
    # p_.region_average_from_post(lat_weights="areacella", weights="land")


# =============================================================================
# calculate rx1day
# =============================================================================


def rx1day():

    p_ = ResampleAnnual(conf.cmip6, "day", "pr", "max", "rx1day")
    p_.annual_from_orig()
    p_.annual_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


def rx1day_monthly():

    p_ = ResampleMonthly(conf.cmip6, "day", "pr", "max", "rx1day_monthly")
    p_.resample_monthly_from_orig()
    p_.regrid_from_post()
    # p_.region_average_from_post(lat_weights="areacella", weights="land")


# =============================================================================
# calculate rx5day
# =============================================================================


def rx5day():

    p_ = RxNday(conf.cmip6, window=5)
    p_.rxnday_from_orig()
    p_.rxnday_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


# =============================================================================
# calculate rx30day
# =============================================================================


def rx30day():

    p_ = RxNday(conf.cmip6, window=30)
    p_.rxnday_from_orig()
    p_.rxnday_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


# =============================================================================
# calculate cdd
# =============================================================================


def cdd():

    p_ = CDD(conf.cmip6)
    p_.cdd_from_orig()
    p_.cdd_from_orig_pi_control()
    p_.regrid_from_post()
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land")


# =============================================================================
# calculate mrso
# =============================================================================


def mrso():

    p_ = NoTransform(conf.cmip6, table="Lmon", varn="mrso", postprocess_name="sm")
    p_.no_transform_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])
    p_.regrid_from_post(method="con")

    p_.resample_seasonal_from_post(
        "mean", invalidate_beg_end=True, from_concat=True, after_regrid=True
    )
    # p_.region_average_from_post(lat_weights="areacella", weights="land_no_ice")


# def mrso_dry_months():
#
#     p_ = ConsecutiveMonthsClimFromOrig(
#         conf.cmip6, table="Lmon", varn="mrso", how="min", postprocess_name="dry_months"
#     )
#
#     p_.consecutive_max_min_months(
#         beg=1850, end=1900, exp="historical", mask_out=["ocean", "landice", "antarctica", GREENLAND]
#     )
#     # only for illustration purposes, use largest area fraction remapping
#     p_.regrid_from_post(method="laf")


def mrso_annmean():

    p_ = ResampleAnnual(conf.cmip6, "Lmon", "mrso", "mean", "sm_annmean")
    p_.annual_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])
    p_.annual_from_orig_pi_control(
        mask_out=["ocean", "landice", "antarctica", GREENLAND]
    )
    p_.regrid_from_post(method="con")
    # for Jérôme Servonnat/ Carley Iles
    p_.regrid_from_post(method="con", target_grid="g010a")

    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land_no_ice")


def mrso_annmean_CMIP5():

    p_ = ResampleAnnual(conf.cmip5, "Lmon", "mrso", "mean", "sm_annmean")
    p_.annual_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])
    p_.regrid_from_post(method="con")
    p_.region_average_from_post(lat_weights="areacella", weights="land_no_ice")


# def mrso_smdd_day():
#
#     p_ = SMDryDaysZhangFromOrig(conf.cmip6, "day", "mrso", "SMdd_q10_day")
#     p_.sm_dry_days_clim_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])


# def mrso_smdd():
#
#     p_ = SMDryDaysZhangFromOrig(conf.cmip6, "Lmon", "mrso", "SMdd_q10")
#     p_.sm_dry_days_clim_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])
#     p_.sm_dry_days_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])
#     p_.sm_dry_days_from_orig_pi_control(mask_out=["ocean", "landice", "antarctica", GREENLAND])
#     p_.regrid_from_post(method="con")
#     p_.iav20_after_regrid_from_post()
#     p_.region_average_from_post(lat_weights="areacella", weights="land_no_ice")


# def mrso_smdd_intensity():
#
#     # "_intensity" is added to the postprocess name!
#     p_ = SMDryDaysIntensityZhangFromOrig(conf.cmip6, "Lmon", "mrso", "SMdd_q10")
#     # get this from mrso_smdd!
#     # p_.sm_dry_days_clim_from_orig(mask_out=["ocean", "landice"])
#
#     p_.sm_dry_days_intensity_from_orig(mask_out=["ocean", "landice"])
#     p_.sm_dry_days_intensity_from_orig_pi_control(mask_out=["ocean", "landice"])
#     p_.regrid_from_post(method="con")
#     p_.iav20_after_regrid_from_post()
#     p_.region_average_from_post(lat_weights="areacella", weights="land_no_ice")


# =============================================================================
# calculate mrsos
# =============================================================================


def mrsos():

    p_ = NoTransform(conf.cmip6, table="Lmon", varn="mrsos", postprocess_name="sm")
    p_.no_transform_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])
    p_.regrid_from_post(method="con")
    p_.region_average_from_post(lat_weights="areacella", weights="land_no_ice")


def mrsos_annmean():

    p_ = ResampleAnnual(conf.cmip6, "Lmon", "mrsos", "mean", "sm_annmean")
    p_.annual_from_orig(mask_out=["ocean", "landice", "antarctica", GREENLAND])
    p_.annual_from_orig_pi_control(
        mask_out=["ocean", "landice", "antarctica", GREENLAND]
    )
    p_.regrid_from_post(method="con")
    p_.iav20_after_regrid_from_post()
    p_.region_average_from_post(lat_weights="areacella", weights="land_no_ice")


def seaice_any_annual():

    with postprocess.ResampleAnnualFromOrig(conf.cmip6) as p:
        p.postprocess_name = "any_annual"
        p.set_files_kwargs(table="SImon", varn="siconc", exp="historical")
        p.transform(how="any")

    with postprocess.ResampleAnnualFromOrig(conf.cmip5) as p:
        p.postprocess_name = "any_annual"
        p.set_files_kwargs(table="OImon", varn="sic", exp="historical")
        p.transform(how="any")


def region_average_arctic_mid_lat():
    # for SPM Sonia & Ed
    from utils import regions

    mid_lat_arctic_region = regions.mid_lat_arctic_region

    with postprocess.RegionAverageFromPost(conf.cmip6) as p:
        p.postprocess_name = "txx_reg_ave_mid_lat_arctic"
        p.set_files_kwargs(varn="tasmax", postprocess="txx", exp="*")
        p.transform(
            regions=mid_lat_arctic_region, lat_weights="areacella", land_only=False
        )

    with postprocess.RegionAverageFromPost(conf.cmip6) as p:
        p.postprocess_name = "txx_monthly_reg_ave_mid_lat_arctic"
        p.set_files_kwargs(varn="tasmax", postprocess="txx_monthly", exp="*")
        p.transform(
            regions=mid_lat_arctic_region, lat_weights="areacella", land_only=False
        )

    with postprocess.RegionAverageFromPost(conf.cmip6) as p:
        p.postprocess_name = "tnn_reg_ave_mid_lat_arctic"
        p.set_files_kwargs(varn="tasmin", postprocess="tnn", exp="*")
        p.transform(
            regions=mid_lat_arctic_region, lat_weights="areacella", land_only=False
        )

    with postprocess.RegionAverageFromPost(conf.cmip6) as p:
        p.postprocess_name = "tnn_monthly_reg_ave_mid_lat_arctic"
        p.set_files_kwargs(varn="tasmin", postprocess="tnn_monthly", exp="*")
        p.transform(
            regions=mid_lat_arctic_region, lat_weights="areacella", land_only=False
        )


# =============================================================================
# calculate TX / TXx for US western heatwave
# =============================================================================


def tx_for_western_us_heatwave():

    from utils import USheatwave2021

    coords = USheatwave2021.region_extended_slice
    region = USheatwave2021.region

    p_ = SelectRegion(
        conf.cmip6,
        "day",
        "tasmax",
        "tx_western_us",
        exp=["historical", "ssp585"],
        ensnumber=None,
        **coords,
    )
    p_.select_region_from_orig()
    p_.region_average_from_post(
        lat_weights="areacella",
        weights="land_no_ice",
        region=region,
        region_name="WNA",
        ensnumber=None,
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

    # client = Client()
    # print(client)

    # parse cmd line arguments
    options = docopt.docopt(main.__doc__, version=None)

    postprocess = options["<postprocess>"]

    functions = {
        "tas_globmean": tas_globmean,
        "tos_globmean": tos_globmean,
        "tas_annmean": tas_annmean,
        "tas_monthly": tas_monthly,
        "tas_summer_months": tas_summer_months,
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
        # "mrso_dry_months": mrso_dry_months,
        "mrso_annmean": mrso_annmean,
        "mrso_annmean_CMIP5": mrso_annmean_CMIP5,
        # "mrso_smdd": mrso_smdd,
        # "mrso_smdd_intensity": mrso_smdd_intensity,
        # "mrso_smdd_day": mrso_smdd_day,
        "mrsos": mrsos,
        "mrsos_annmean": mrsos_annmean,
        "seaice_any_annual": seaice_any_annual,
        "region_average_arctic_mid_lat": region_average_arctic_mid_lat,
        "tx_for_western_us_heatwave": tx_for_western_us_heatwave,
    }

    func = functions[postprocess]

    func()


if __name__ == "__main__":
    main()
