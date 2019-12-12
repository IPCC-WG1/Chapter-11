# compute year of warming level

from datetime import datetime

import conf

from utils import computation


def warming_level_to_str(warming_level):
    return str(warming_level).replace(".", "")


def warming_level_years(
    tas_list, warming_level, print_warming_level=True, add_grid_info=False
):

    out = f"# warming level: {warming_level}°C above 1850-1900\n"

    if print_warming_level:
        out += "warming_level_{}:\n".format(warming_level_to_str(warming_level))

    for ds, metadata in tas_list:

        beg, end, mid = computation.calc_year_of_warming_level(ds.tas, warming_level)

        fmt = " {{model: {model}, ensemble: {ens}, exp: {exp}"

        if add_grid_info:
            fmt += ", grid: {grid}"

        if beg:
            fmt = "-" + fmt + ", start_year: {beg}, end_year: {end}}}\n"
            out += fmt.format(beg=beg, end=end, **metadata)
        else:
            fmt = "#" + fmt + "}} -- did not reach {warming_level}°C\n"
            out += fmt.format(warming_level=warming_level, **metadata)

    out += "\n"

    return out


def write_info(fid):
    fid.write(f"# author: Mathias Hauser (mathias.hauser@env.ethz.ch)\n")
    fid.write(f"# script: warming_levels.py\n")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S\n")
    fid.write(f"# executed: {now}\n")


def write_warming_level_to_file(tas_list, conf_cmip, add_grid_info=False):

    # all warming levels in the same file

    if add_grid_info:
        fN = conf_cmip.warming_levels_folder + "warming_levels_one_ens_grid.yml"
    else:
        fN = conf_cmip.warming_levels_folder + "warming_levels_one_ens.yml"

    with open(fN, "w") as fid:
        write_info(fid)
        for warming_level in WARMING_LEVELS:
            string = warming_level_years(
                tas_list, warming_level, add_grid_info=add_grid_info
            )
            fid.write(string)

    # one file for all warming levels

    # for warming_level in WARMING_LEVELS:

    #     fN = conf_cmip.warming_levels_folder
    #     fN += "warming_levels_one_ens_{}.yml".format(
    #         warming_level_to_str(warming_level)
    #     )

    #     with open(fN, "w") as fid:
    #         write_info(fid)
    #         string = warming_level_years(
    #             tas_list, warming_level, print_warming_level=False
    #         )
    #         fid.write(string)


WARMING_LEVELS = [1.0, 1.5, 2.0, 3.0, 4.0, 0.61]

if __name__ == "__main__":

    # load data
    c5_tas = conf.cmip5.load_postprocessed_all_concat(
        varn="tas", postprocess="global_mean"
    )

    c6_tas = conf.cmip6.load_postprocessed_all_concat(
        varn="tas", postprocess="global_mean"
    )

    # write warming levels to file
    write_warming_level_to_file(c5_tas, conf.cmip5)

    write_warming_level_to_file(c6_tas, conf.cmip6)

    write_warming_level_to_file(c6_tas, conf.cmip6, add_grid_info=True)
