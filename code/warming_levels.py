# compute year of warming level

import os
from datetime import datetime

import conf
from utils import computation


def warming_level_to_str(warming_level):
    return str(warming_level).replace(".", "")


def _fmt_str_yml(beg, end, warming_level, add_grid_info, **meta):
    fmt = " {{model: {model}, ensemble: {ens}, exp: {exp}"

    if add_grid_info:
        fmt += ", grid: {grid}"

    if beg:
        fmt = "-" + fmt + ", start_year: {beg}, end_year: {end}}}\n"
        out = fmt.format(beg=beg, end=end, **meta)
    else:
        fmt = "#" + fmt + "}} -- did not reach {warming_level}°C\n"
        out = fmt.format(warming_level=warming_level, **meta)

    return out


def _fmt_str_csv(beg, end, warming_level, add_grid_info, **meta):
    fmt = "{model}, {ens}, {exp}"

    if add_grid_info:
        fmt += ", {grid}"

    if beg:
        fmt = fmt + ", {warming_level}, {beg}, {end}\n"
        out = fmt.format(beg=beg, end=end, warming_level=warming_level, **meta)
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
    check_years=True,
):

    out_yml = f"# warming level: {warming_level}°C above {start_clim}-{end_clim}\n"
    how = "absolute"

    if not check_years:
        how = "no_check_absolute"

    out_csv = ""

    # creates yml dict
    if print_warming_level:
        out_yml += "warming_level_{}:\n".format(warming_level_to_str(warming_level))

    for ds, meta in tas_list:

        anomaly = computation.calc_anomaly(
            ds.tas, start=start_clim, end=end_clim, meta=meta, how=how
        )

        # skip if too short
        if not len(anomaly):
            continue

        beg, end, mid = computation.calc_year_of_warming_level(anomaly, warming_level)

        out_yml += _fmt_str_yml(beg, end, warming_level, add_grid_info, **meta)
        out_csv += _fmt_str_csv(beg, end, warming_level, add_grid_info, **meta)

    out_yml += "\n"

    return out_yml, out_csv


def write_info(fid, start_clim, end_clim, check_years):
    fid.write("# author: Mathias Hauser (mathias.hauser@env.ethz.ch)\n")
    fid.write("# script: warming_levels.py\n")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fid.write(f"# executed: {now}\n")
    fid.write(f"# reference years: {start_clim} to {end_clim}\n")

    if not check_years:
        fid.write(f"# NOTE: may include simulations starting after {start_clim}!\n")


def _get_filename(
    conf_cmip,
    start_clim,
    end_clim,
    all_ens,
    check_years,
    add_grid_info,
    file_ending,
    subfolder="",
    what="warming_levels",
):

    one_or_all = "one_ens"
    folder_one_or_all = ""
    check_years_ = ""

    if all_ens:
        one_or_all = "all_ens"
        folder_one_or_all = "_all_ens"

    if not check_years:
        check_years_ = "_no_bounds_check"

    folder = conf_cmip.warming_levels_folder
    grid = "_grid" if add_grid_info else ""

    cmip = conf_cmip.cmip

    fN = os.path.join(
        folder + folder_one_or_all,
        subfolder,
        f"{cmip}_{what}_{one_or_all}_{start_clim}_{end_clim}{check_years_}{grid}.{file_ending}",
    )

    return fN


def write_start_years_no_boundscheck(
    conf_cmip, tas_list, start_clim, end_clim, all_ens
):

    fN = _get_filename(
        conf_cmip,
        start_clim,
        end_clim,
        all_ens,
        False,
        add_grid_info=False,
        file_ending="txt",
        subfolder="",
        what="start_years",
    )

    txt = ""

    txt += "# author: Mathias Hauser (mathias.hauser@env.ethz.ch)\n"
    txt += "# script: warming_levels.py\n"
    txt += f"# reference years: {start_clim} to {end_clim}\n"
    txt += f"# start_years of models starting AFTER {start_clim}\n"
    txt += "model, ensemble, exp, start, end_year\n"

    models = list()
    for ds, meta in tas_list:

        anomaly = computation.calc_anomaly(
            ds.tas, start=start_clim, end=end_clim, meta=meta, how="absolute"
        )

        fmt = "{model}, {ens}, {exp}, {start}, {end_clim}\n"

        # skip if too short
        if not len(anomaly):
            start = ds.year[0].item()

            models.append(fmt.format(start=start, end_clim=end_clim, **meta))

    # sort the by model
    txt += "".join(sorted(models))

    with open(fN, "w") as fid:
        fid.writelines(txt)


def write_warming_level_to_file(
    tas_list,
    conf_cmip,
    warming_levels,
    clim=None,
    add_grid_info=False,
    all_ens=False,
    check_years=True,
):

    # all warming levels in the same file

    if clim is None:
        start_clim = conf_cmip.ANOMALY_YR_START
        end_clim = conf_cmip.ANOMALY_YR_END
    else:
        start_clim, end_clim = clim

    fN_yml = _get_filename(
        conf_cmip, start_clim, end_clim, all_ens, check_years, add_grid_info, "yml"
    )
    file_ending = subfolder = "csv"
    fN_csv = _get_filename(
        conf_cmip,
        start_clim,
        end_clim,
        all_ens,
        check_years,
        add_grid_info,
        file_ending,
        subfolder,
    )

    string_yml = ""
    string_csv = ""
    for warming_level in warming_levels:
        string_yml_, string_csv_ = warming_level_years(
            tas_list,
            warming_level,
            start_clim,
            end_clim,
            add_grid_info=add_grid_info,
            check_years=check_years,
        )

        string_yml += string_yml_
        string_csv += string_csv_

    with open(fN_yml, "w") as fid:
        write_info(fid, start_clim, end_clim, check_years)
        fid.write("\n")
        fid.writelines(string_yml)

    grid = "grid, " if add_grid_info else ""
    header = f"model, ensemble, exp, {grid}warming_level, start_year, end_year\n"

    with open(fN_csv, "w") as fid:
        write_info(fid, start_clim, end_clim, check_years)
        fid.write(header)
        fid.writelines(string_csv)

    if not check_years:
        write_start_years_no_boundscheck(
            conf_cmip, tas_list, start_clim, end_clim, all_ens
        )


def write_cmip5(warming_levels1, warming_levels2):

    # load data
    c5_tas = conf.cmip5.load_post_all_concat(
        varn="tas",
        postprocess="global_mean",
        anomaly="no_check_no_anom",
        ensnumber=None,
        #         ensnumber=0,
    )

    # ======================
    # all ensnumbers
    write_warming_level_to_file(c5_tas, conf.cmip5, warming_levels1, all_ens=True)
    write_warming_level_to_file(
        c5_tas, conf.cmip5, warming_levels1, all_ens=True, check_years=False
    )
    write_warming_level_to_file(
        c5_tas, conf.cmip5, warming_levels2, clim=(1995, 2014), all_ens=True
    )
    write_warming_level_to_file(
        c5_tas, conf.cmip5, warming_levels1, clim=(1861, 1900), all_ens=True
    )

    # only ensnumber = 0
    c5_tas = computation.select_by_metadata(c5_tas, ensnumber=0)

    write_warming_level_to_file(c5_tas, conf.cmip5, warming_levels1, all_ens=False)
    write_warming_level_to_file(
        c5_tas, conf.cmip5, warming_levels1, all_ens=False, check_years=False
    )
    write_warming_level_to_file(
        c5_tas, conf.cmip5, warming_levels2, clim=(1995, 2014), all_ens=False
    )
    write_warming_level_to_file(
        c5_tas, conf.cmip5, warming_levels1, clim=(1861, 1900), all_ens=False
    )


def write_cmip6(warming_levels1, warming_levels2, all_ens=False):

    ensnumber = 0
    if all_ens:
        ensnumber = None

    c6_tas = conf.cmip6.load_post_all_concat(
        varn="tas",
        postprocess="global_mean",
        ensnumber=ensnumber,
        anomaly="no_check_no_anom",
    )

    # write warming levels to file

    write_warming_level_to_file(c6_tas, conf.cmip6, warming_levels1, all_ens=all_ens)

    write_warming_level_to_file(
        c6_tas, conf.cmip6, warming_levels1, add_grid_info=True, all_ens=all_ens
    )

    write_warming_level_to_file(
        c6_tas, conf.cmip6, warming_levels2, clim=(1995, 2014), all_ens=all_ens
    )

    write_warming_level_to_file(
        c6_tas,
        conf.cmip6,
        warming_levels2,
        clim=(1995, 2014),
        add_grid_info=True,
        all_ens=all_ens,
    )


if __name__ == "__main__":

    warming_levels1 = [1.0, 1.5, 2.0, 3.0, 4.0, 0.61]
    # for Chapter 4
    warming_levels2 = [0.94, 3.43]

    # cmip5 is not changing
    #     write_cmip5(warming_levels1, warming_levels2)

    write_cmip6(warming_levels1, warming_levels2)

    write_cmip6(warming_levels1, warming_levels2, all_ens=True)
