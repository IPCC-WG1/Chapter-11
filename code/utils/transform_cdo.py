import os
import cdo

# import xesmf
# import xarray
import logging

# import subprocess
# import numpy as np

from .file_utils import _any_file_does_not_exist, _file_exists

logger = logging.getLogger(__name__)

CDO = cdo.Cdo()
METHOD_STR = {
    "bil": "Bilenear interpolation",
    "con2": "Second order conservative remapping",
    "dis": "Distance-weighted average remapping",
}


def delete_corrupt_files(filename):
    if not os.path.isfile(filename):
        return False
    try:
        CDO.info(filename)
    except cdo.CDOException:
        logmsg = "\n".join(
            [
                "Deleting corrupt file:",
                f"  {filename}",
                "  Re-run script to create it again",
            ]
        )
        logger.warning(logmsg)
        os.remove(filename)
        return True
    return False


def _regrid_cdo(fN_in, fN_out, target_grid, method):
    if method == "bil":
        CDO.remapbil(
            f"../grids/{target_grid}.txt", options="-b F64", input=fN_in, output=fN_out
        )
    elif method == "con2":
        CDO.remapcon2(
            f"../grids/{target_grid}.txt", options="-b F64", input=fN_in, output=fN_out
        )
    elif method == "dis":
        CDO.remapdis(
            f"../grids/{target_grid}.txt", options="-b F64", input=fN_in, output=fN_out
        )
    else:
        raise NotImplementedError


def regrid_cdo(
    fN_in, fN_out, target_grid, overwrite=False, method="con2", cdo_version="1.9.8"
):

    if isinstance(cdo_version, str):
        cdo_version = [cdo_version]
    if cdo.getCdoVersion("cdo") not in cdo_version:
        errmsg = f'cdo: {cdo.getCdoVersion("cdo")} not in {cdo_version}'
        logger.error(errmsg)
        raise ValueError(errmsg)

    if overwrite and _file_exists(fN_out):
        os.remove(fN_out)  # delete old file to avoid permission errors

    if overwrite or _any_file_does_not_exist(fN_out):

        logger.info(f"Process target file:\n  {fN_out}")
        try:
            _regrid_cdo(fN_in, fN_out, target_grid, method)
        except cdo.CDOException:
            if _file_exists(fN_out):
                os.remove(fN_out)  # make sure no broken file is left
            logger.error(f"cdo error! Regridding failed for source:\n  {fN_out}")
            delete_corrupt_files(fN_out)
            return None
