# compute year of warming level

from datetime import datetime

import conf
from utils import computation


def warming_level_to_str(warming_level):
    return str(warming_level).replace(".", "")


def _fmt_str_yml(beg, end, warming_level, add_grid_info, **metadata):
    fmt = " {{model: {model}, ensemble: {ens}, exp: {exp}"

    if add_grid_info:
        fmt += ", grid: {grid}"

    if beg:
        fmt = "-" + fmt + ", start_year: {beg}, end_year: {end}}}\n"
        out = fmt.format(beg=beg, end=end, **metadata)
    else:
        fmt = "#" + fmt + "}} -- did not reach {warming_level}°C\n"
        out = fmt.format(warming_level=warming_level, **metadata)

    return out


def _fmt_str_csv(beg, end, warming_level, add_grid_info, **metadata):
    fmt = "{model}, {ens}, {exp}"

    if add_grid_info:
        fmt += ", {grid}"

    if beg:
        fmt = fmt + ", {warming_level}, {beg}, {end}\n"
        out = fmt.format(beg=beg, end=end, warming_level=warming_level, **metadata)
    else:
        out = ""

    return out


def warming_level_years(
    tas_list,
    warming_level,
    start_clim,
    end_clim,
    print_warming_level=True,
    add_grid_info=False,
):

    out_yml = f"# warming level: {warming_level}°C above {start_clim}-{end_clim}\n"
    out_csv = ""

    # creates yml dict
    if print_warming_level:
        out_yml += "warming_level_{}:\n".format(warming_level_to_str(warming_level))

    for ds, metadata in tas_list:

        anomaly = computation.calc_anomaly(
            ds.tas, start=start_clim, end=end_clim, metadata=metadata, how="absolute"
        )

        beg, end, mid = computation.calc_year_of_warming_level(anomaly, warming_level)

        out_yml += _fmt_str_yml(beg, end, warming_level, add_grid_info, **metadata)
        out_csv += _fmt_str_csv(beg, end, warming_level, add_grid_info, **metadata)

    out_yml += "\n"

    return out_yml, out_csv


def write_info(fid):
    fid.write("# author: Mathias Hauser (mathias.hauser@env.ethz.ch)\n")
    fid.write("# script: warming_levels.py\n")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fid.write(f"# executed: {now}\n")


def write_warming_level_to_file(
    tas_list, conf_cmip, warming_levels, clim=None, add_grid_info=False
):

    # all warming levels in the same file

    if clim is None:
        start_clim = conf_cmip.ANOMALY_YR_START
        end_clim = conf_cmip.ANOMALY_YR_END
    else:
        start_clim, end_clim = clim

    folder = conf_cmip.warming_levels_folder
    grid = "_grid" if add_grid_info else ""

    cmip = conf_cmip.cmip

    fN_yml = folder + f"{cmip}_warming_levels_one_ens_{start_clim}_{end_clim}{grid}.yml"

    fN_csv = (
        folder + f"csv/{cmip}_warming_levels_one_ens_{start_clim}_{end_clim}{grid}.csv"
    )

    string_yml = ""
    string_csv = ""
    for warming_level in warming_levels:
        string_yml_, string_csv_ = warming_level_years(
            tas_list, warming_level, start_clim, end_clim, add_grid_info=add_grid_info,
        )

        string_yml += string_yml_
        string_csv += string_csv_

    with open(fN_yml, "w") as fid:
        write_info(fid)
        fid.write("\n")
        fid.writelines(string_yml)

    header = "model, ensemble, exp, "
    grid = "grid, " if add_grid_info else ""
    header += grid + "warming_level, start_year, end_year\n"

    with open(fN_csv, "w") as fid:
        write_info(fid)
        fid.write(header)
        fid.writelines(string_csv)


def write_cmip5(warming_levels1, warming_levels2):

    # load data
    c5_tas = conf.cmip5.load_postprocessed_all_concat(
        varn="tas", postprocess="global_mean"
    )

    write_warming_level_to_file(c5_tas, conf.cmip5, warming_levels1)

    # write warming levels to file
    write_warming_level_to_file(c5_tas, conf.cmip5, warming_levels2, clim=(1995, 2014))


if __name__ == "__main__":

    warming_levels1 = [1.0, 1.5, 2.0, 3.0, 4.0, 0.61]
    # for Chapter 4
    warming_levels2 = [0.94, 3.43]

    # cmip5 is not changing
    write_cmip5(warming_levels1, warming_levels2)

    c6_tas = conf.cmip6.load_postprocessed_all_concat(
        varn="tas", postprocess="global_mean"
    )

    # write warming levels to file

    write_warming_level_to_file(c6_tas, conf.cmip6, warming_levels1)

    write_warming_level_to_file(c6_tas, conf.cmip6, warming_levels1, add_grid_info=True)

    write_warming_level_to_file(c6_tas, conf.cmip6, warming_levels2, clim=(1995, 2014))

    write_warming_level_to_file(
        c6_tas, conf.cmip6, warming_levels2, clim=(1995, 2014), add_grid_info=True
    )
