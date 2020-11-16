import conf
from utils import computation


def prepare_and_write_tglob(ds, metadata, conf_cmip):

    # the reference period
    start_clim = conf_cmip.ANOMALY_YR_START
    end_clim = conf_cmip.ANOMALY_YR_END

    # calc anomaly wrt 1850 to 1900
    anomaly = computation.calc_anomaly(
        ds.tas, start=start_clim, end=end_clim, metadata=metadata, how="absolute"
    )

    # calc running mean
    anomaly_rolling = anomaly.rolling(year=20, center=True).mean()

    # assign to ds
    ds["tas_anomaly"] = anomaly
    ds["tas_anomaly_rolling"] = anomaly_rolling

    # get filename out
    metadata.pop("postprocess", None)
    fN_out = conf_cmip.files_post.create_full_name(
        postprocess="for_warming_lvls", **metadata
    )

    # save
    ds.to_netcdf(fN_out)

    return ds


if __name__ == "__main__":

    # models to process
    models = ["FGOALS-g3", "NESM3"]

    # load Tglob
    c6_tas = conf.cmip6.load_postprocessed_all_concat(
        varn="tas", postprocess="global_mean", model=models, anomaly="no_anom",
    )

    # save the data
    for ds, meta in c6_tas:
        prepare_and_write_tglob(ds, meta, conf.cmip6)
