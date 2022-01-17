# code vendored from cmip6-ng under the conditions of their license
# https://git.iac.ethz.ch/cmip6-ng/cmip6-ng
# see licenses/CMIP6_NG_LICENSE

import logging
import os

import cdo

logger = logging.getLogger(__name__)

CDO = cdo.Cdo()
METHOD_STR = {
    "bil": "Bilenear interpolation",
    "con2": "Second order conservative remapping",
    "con": "Conservative remapping",
    "dis": "Distance-weighted average remapping",
    "laf": "Largest area fraction remapping",
}


def delete_corrupt_files(filename):
    if not os.path.isfile(filename):
        return False
    try:
        CDO.info(filename)
    except cdo.CDOException:
        logmsg = f"Deleting corrupt file: {filename}\nRe-run script to create it again"
        logger.warning(logmsg)
        os.remove(filename)
        return True
    return False


def _regrid_cdo(fN_in, fN_out, target_grid, method):

    func = getattr(CDO, f"remap{method}")
    func(f"../grids/{target_grid}.txt", options="-b F64", input=fN_in, output=fN_out)


def regrid_cdo(
    fN_in, fN_out, target_grid, overwrite=False, method="con2", cdo_version="1.9.8"
):
    """regrid a netCDF file using cdo remap*

    Parameters
    ----------
    fN_in : str
        File name of netCDF file to regrid.
    fN_out : str
        Name to save the regridded file under.
    target_grid : str
        Name of target_grid. Grid definition must reside in ../grids/
    overwrite : bool, default False
        Overwrites fN_out if it already exists.
    method : str, default: "con2"
        Method to use for regridding.
    cdo_version : str, optional
        Only this version of cdo is allowed to be used.
    """

    if isinstance(cdo_version, str):
        cdo_version = [cdo_version]
    if cdo.getCdoVersion("cdo") not in cdo_version:
        errmsg = f'cdo: {cdo.getCdoVersion("cdo")} not in {cdo_version}'
        logger.error(errmsg)
        raise ValueError(errmsg)

    if overwrite and os.path.isfile(fN_out):
        os.remove(fN_out)  # delete old file to avoid permission errors

    if overwrite or not os.path.isfile(fN_out):

        logger.info(f"Process target file:\n  {fN_out}")
        try:
            _regrid_cdo(fN_in, fN_out, target_grid, method)
        except cdo.CDOException:
            if os.path.isfile(fN_out):
                os.remove(fN_out)  # make sure no broken file is left
            logger.error(f"cdo error! Regridding failed for source:\n  {fN_out}")
            delete_corrupt_files(fN_out)
