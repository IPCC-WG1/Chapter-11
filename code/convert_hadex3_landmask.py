import warnings
from datetime import datetime
from os import path

import pandas as pd
import xarray as xr

# we would get 13 warnings otherwise
warnings.filterwarnings("ignore", message="variable '.*' has multiple fill values")

VERSION = "v3.0.2"
BASE_PATH = f"../../data/HadEX3/{VERSION}/raw"


def convert_hadex3_landmask():
    """convert HadEX3 landfrac from 1D csv to 2D netCDF landmask"""

    # load TXx dataset for the coordinates and attributes
    fN = path.join(BASE_PATH, "HadEX3_TXx_1901-2018_ADW_61-90_1.25x1.875deg.nc")
    ds = xr.open_dataset(fN)

    # load landmask text file
    fN = path.join(BASE_PATH, "land_mask_144x192.msk")

    df = pd.read_csv(
        fN,
        header=None,
        sep=" ",
        skipinitialspace=True,
        names=["lat", "lon", "land", "all_100"],
    )

    # convert to 2D array
    landfrac = df["land"].values.reshape(144, 192)

    # convert to landmask
    landmask = landfrac >= 50

    # create xr.Dataset

    landmask = xr.Dataset(
        data_vars=dict(
            landmask=((("latitude", "longitude"), landmask)),
            longitude_bounds=ds.longitude_bounds,
            latitude_bounds=ds.latitude_bounds,
        ),
        coords=dict(
            latitude=ds.latitude,
            longitude=ds.longitude,
        ),
    )

    # add & update global attributes
    landmask.attrs = ds.attrs

    now = datetime.now()
    landmask.attrs["date_created"] = datetime.strftime(now, "%a %b %d, %H:%m %Y")
    landmask.attrs["title"] = "landmask"
    landmask.attrs["history"] = "Created by convert_hadex3_landmask.py"
    landmask.attrs["creator_name"] = "Mathias Hauser & Robert Dunn"
    landmask.attrs["creator_url"] = "www.iac.ethz.ch & www.metoffice.gov.uk"
    landmask.attrs[
        "creator_email"
    ] = "mathias.hauser@env.ethz.ch & robert.dunn@metoffice.gov.uk"

    # remove unneccesary attributes
    del landmask.attrs["processing_level"]
    del landmask.attrs["time_coverage_start"]
    del landmask.attrs["time_coverage_end"]
    del landmask.attrs["time_coverage_resolution"]

    # add variable attributes
    landmask.landmask.attrs["long_name"] = "landmask"
    landmask.landmask.attrs["units"] = "1"

    # landmask.landmask.attrs["standard_name"]

    # ensure we don't get NaN as _FillValue
    for var in ["latitude", "longitude", "longitude_bounds", "latitude_bounds"]:
        landmask[var].encoding["_FillValue"] = -99.9

    # write netCDF
    fN_out = path.join(
        BASE_PATH, "HadEX3_landmask_1901-2018_ADW_XX-XX_1.25x1.875deg.nc"
    )
    landmask.to_netcdf(fN_out)


if __name__ == "__main__":
    convert_hadex3_landmask()
