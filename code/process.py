import logging
import docopt

from utils.transform import Globmean, ResampleAnnual, RegionAverage
from utils.transform_cdo import regrid_cdo
import fixes
import filefinder as ff
from utils import xarray_utils as xru
from utils import mkdir
from utils import regions

import conf

logger = logging.getLogger(__name__)

# from dask.distributed import Client
# client = Client()  # set up local cluster
# print(client)


# =============================================================================
# define class to postprocess cmip5 and cmip6 data
# =============================================================================


class _ProcessCmipData:
    """docstring for ProcessCmipData"""

    def __init__(self, conf_cmip, fixes_data, fixes_files):

        self.conf_cmip = conf_cmip
        self.fixes_data = fixes_data
        self.fixes_files = fixes_files

    def _create_folder_for_output(self, files, postprocess_name):

        __, metadata = files[0]

        folder_out = self.conf_cmip.files_post.create_path_name(
            **metadata, postprocess=postprocess_name
        )
        mkdir(folder_out)

    def find_input_orig(self, table, varn, exp, **kwargs):

        find_path = self.conf_cmip.files_orig.find_paths

        return self._find_input(find_path, varn=varn, exp=exp, table=table, **kwargs)

    def find_input_post(self, postprocess, varn, exp, **kwargs):

        find_path = self.conf_cmip.files_post.find_files

        return self._find_input(
            find_path, postprocess=postprocess, varn=varn, exp=exp, **kwargs
        )

    def _find_input(self, find_path, varn, exp, **kwargs):

        # if no experiment name is given use tier1 list incl historical
        if exp is None:
            exp = self.conf_cmip.scenarios_incl_hist

        ensnumber = kwargs.pop("ensnumber", 0)

        files = find_path(varn=varn, exp=exp, **kwargs)

        files = ff.cmip.parse_ens(files)
        files = ff.cmip.create_ensnumber(files)
        files = files.search(ensnumber=ensnumber)

        return files

    def postprocess_from_orig(
        self, table, varn, postprocess_name, transform_func, exp=None, **kwargs
    ):

        files = self.find_input_orig(table=table, varn=varn, exp=exp, **kwargs)

        self._create_folder_for_output(files, postprocess_name)

        for folder_in, metadata in files:

            fN_out = self.conf_cmip.files_post.create_full_name(
                **metadata, postprocess=postprocess_name
            )

            print(metadata)
            print(folder_in)

            xru.postprocess(
                fN_out,
                fNs_in_or_creator=self.fixes_files(folder_in),
                metadata=metadata,
                transform_func=transform_func,
                fixes=self.fixes_data,
            )

    def global_mean_from_orig(
        self, table, varn, postprocess_name="global_mean", exp=None, **kwargs
    ):

        transform_func = (Globmean(var=varn),)

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

    def resample_annual_quantile_from_orig(
        self, table, varn, postprocess_name, q, exp=None, **kwargs
    ):

        transform_func = ResampleAnnual(var=varn, how="quantile", q=q)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def region_average_from_orig(
        self, table, varn, postprocess_name, exp=None, **kwargs
    ):

        transform_func = RegionAverage(varn, regions=regions.ar6_region)

        return self.postprocess_from_orig(
            table, varn, postprocess_name, transform_func, exp=exp, **kwargs
        )

    def regrid_from_post(
        self, varn, postprocess_before, postprocess_name, exp=None, **kwargs
    ):

        files = self.find_input_post(
            postprocess=postprocess_before, varn=varn, exp=exp, **kwargs
        )

        files.df = files.df.drop("postprocess", axis=1)

        self._create_folder_for_output(files, postprocess_name)

        for fN_in, metadata in files:

            fN_out = self.conf_cmip.files_post.create_full_name(
                **metadata, postprocess=postprocess_name
            )

            print(metadata)
            regrid_cdo(fN_in, fN_out, "g025")

    def region_average_from_post(
        self, varn, postprocess_before, postprocess_name, exp=None, **kwargs
    ):

        files = self.find_input_post(
            postprocess=postprocess_before, varn=varn, exp=exp, **kwargs
        )

        files.df = files.df.drop("postprocess", axis=1)

        self._create_folder_for_output(files, postprocess_name)

        transform_func = RegionAverage(varn, regions=regions.ar6_region)

        for fN_in, metadata in files:

            fN_out = self.conf_cmip.files_post.create_full_name(
                **metadata, postprocess=postprocess_name
            )

            xru.postprocess(fN_out, fN_in, metadata, transform_func=transform_func)


# =============================================================================
# instantiate the postprocess class
# =============================================================================


process_cmip5_data = _ProcessCmipData(conf.cmip5, fixes.cmip5_data, fixes.cmip5_files)
process_cmip6_data = _ProcessCmipData(conf.cmip6, fixes.cmip6_data, fixes.cmip6_files)


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


def tas_reg_ave():

    process_cmip5_data.region_average_from_orig(
        table="Amon",
        varn="tas",
        postprocess_name="reg_ave_ar6",
        exp=conf.cmip5.scenarios_all_incl_hist,
        ensnumber=0,
    )

    process_cmip6_data.region_average_from_orig(
        table="Amon",
        varn="tas",
        postprocess_name="reg_ave_ar6",
        exp=conf.cmip6.scenarios_all_incl_hist,
        ensnumber=0,
    )


# # =============================================================================
# # calculate txx
# # =============================================================================


def txx():

    process_cmip5_data.resample_annual_from_orig(
        table="day", varn="tasmax", postprocess_name="txx", how="max", exp=None
    )

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="tasmax", postprocess_name="txx", how="max", exp=None
    )

    process_cmip5_data.resample_annual_from_orig(
        table="day", varn="tasmax", postprocess_name="txx", how="max", exp="piControl"
    )

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="tasmax", postprocess_name="txx", how="max", exp="piControl"
    )

    # # regrid txx
    # # =============================================================================

    process_cmip5_data.regrid_from_post(
        varn="tasmax", postprocess_before="txx", postprocess_name="txx_regrid", exp="*"
    )

    process_cmip6_data.regrid_from_post(
        varn="tasmax", postprocess_before="txx", postprocess_name="txx_regrid", exp="*"
    )

    # # region average txx
    # # =============================================================================

    process_cmip5_data.region_average_from_post(
        varn="tasmax",
        postprocess_before="txx",
        postprocess_name="txx_reg_ave_ar6",
        exp="*",
    )
    process_cmip6_data.region_average_from_post(
        varn="tasmax",
        postprocess_before="txx",
        postprocess_name="txx_reg_ave_ar6",
        exp="*",
    )


# # =============================================================================
# # calculate txp95
# # =============================================================================


def txp95():

    process_cmip5_data.resample_annual_quantile_from_orig(
        table="day", varn="tasmax", postprocess_name="txp95", q=0.95, exp=None
    )

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

    process_cmip5_data.regrid_from_post(
        varn="tasmax",
        postprocess_before="txp95",
        postprocess_name="txp95_regrid",
        exp="*",
    )

    process_cmip6_data.regrid_from_post(
        varn="tasmax",
        postprocess_before="txp95",
        postprocess_name="txp95_regrid",
        exp="*",
    )

    # # region average txp95
    # # =============================================================================

    process_cmip5_data.region_average_from_post(
        varn="tasmax",
        postprocess_before="txp95",
        postprocess_name="txp95_reg_ave_ar6",
        exp="*",
    )
    process_cmip6_data.region_average_from_post(
        varn="tasmax",
        postprocess_before="txp95",
        postprocess_name="txp95_reg_ave_ar6",
        exp="*",
    )


# # =============================================================================
# # calculate tnn
# # =============================================================================


def tnn():

    transform_func = ResampleAnnual(var="tasmin", how="min")

    process_cmip5_data.postprocess_from_orig(
        table="day",
        varn="tasmin",
        postprocess_name="tnn",
        transform_func=transform_func,
        exp=None,
    )

    process_cmip6_data.postprocess_from_orig(
        table="day",
        varn="tasmin",
        postprocess_name="tnn",
        transform_func=transform_func,
        exp=None,
    )

    process_cmip5_data.postprocess_from_orig(
        table="day",
        varn="tasmin",
        postprocess_name="tnn",
        transform_func=transform_func,
        exp="piControl",
    )

    process_cmip6_data.postprocess_from_orig(
        table="day",
        varn="tasmin",
        postprocess_name="tnn",
        transform_func=transform_func,
        exp="piControl",
    )

    # # regrid tnn
    # # =============================================================================

    process_cmip5_data.regrid_from_post(
        varn="tasmin", postprocess_before="tnn", postprocess_name="tnn_regrid", exp="*"
    )

    process_cmip6_data.regrid_from_post(
        varn="tasmin", postprocess_before="tnn", postprocess_name="tnn_regrid", exp="*"
    )

    # # region average tnn
    # # =============================================================================

    process_cmip5_data.region_average_from_post(
        varn="tasmin",
        postprocess_before="tnn",
        postprocess_name="tnn_reg_ave_ar6",
        exp="*",
    )
    process_cmip6_data.region_average_from_post(
        varn="tasmin",
        postprocess_before="tnn",
        postprocess_name="tnn_reg_ave_ar6",
        exp="*",
    )


# =============================================================================
# calculate rx1day
# =============================================================================


def rx1day():

    process_cmip5_data.resample_annual_from_orig(
        table="day", varn="pr", postprocess_name="rx1day", how="max", exp=None
    )

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="pr", postprocess_name="rx1day", how="max", exp=None
    )

    process_cmip5_data.resample_annual_from_orig(
        table="day", varn="pr", postprocess_name="rx1day", how="max", exp="piControl"
    )

    process_cmip6_data.resample_annual_from_orig(
        table="day", varn="pr", postprocess_name="rx1day", how="max", exp="piControl"
    )

    # regrid rx1day
    # =============================================================================

    process_cmip5_data.regrid_from_post(
        varn="pr",
        postprocess_before="rx1day",
        postprocess_name="rx1day_regrid",
        exp="*",
    )
    process_cmip6_data.regrid_from_post(
        varn="pr",
        postprocess_before="rx1day",
        postprocess_name="rx1day_regrid",
        exp="*",
    )

    # region average rx1day
    # =============================================================================

    process_cmip5_data.region_average_from_post(
        varn="pr",
        postprocess_before="rx1day",
        postprocess_name="rx1day_reg_ave_ar6",
        exp="*",
    )
    process_cmip6_data.region_average_from_post(
        varn="pr",
        postprocess_before="rx1day",
        postprocess_name="rx1day_reg_ave_ar6",
        exp="*",
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

    # parse cmd line arguments
    options = docopt.docopt(main.__doc__, version=None)

    postprocess = options["<postprocess>"]

    if postprocess == "tas_globmean":
        tas_globmean()

    if postprocess == "tas_reg_ave":
        tas_reg_ave()

    if postprocess == "txx":
        txx()

    if postprocess == "txp95":
        txp95()

    if postprocess == "tnn":
        tnn()

    if postprocess == "rx1day":
        rx1day()


if __name__ == "__main__":
    main()
