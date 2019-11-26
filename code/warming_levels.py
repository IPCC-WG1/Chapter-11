# compute year of warming level

from datetime import datetime

import conf

from utils import computation


def warming_level_to_str(warming_level):
    return str(warming_level).replace(".", "")


def warming_level_years(tas_list, warming_level, print_warming_level=True):

    out = f"# warming level: {warming_level}°C above 1850-1900\n"

    if print_warming_level:
        out += "warming_level_{}:\n".format(warming_level_to_str(warming_level))

    for ds, metadata in tas_list:

        beg, end, mid = computation.calc_year_of_warming_level(ds.tas, warming_level)

        if beg:
            msg = "- {{model: {model}, ensemble: {ens}, exp: {exp}, start_year: {beg}, end_year: {end}}}\n"
            out += msg.format(beg=beg, end=end, **metadata)
        else:
            msg = "# {{model: {model}, ensemble: {ens}, exp: {exp}}} -- did not reach {warming_level}°C\n"
            out += msg.format(warming_level=warming_level, **metadata)

    out += "\n"

    return out


def write_info(fid):
    fid.write(f"# author: Mathias Hauser\n")
    fid.write(f"# script: warming_levels.py\n")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S\n")
    fid.write(f"# executed: {now}\n")


def write_warming_level_to_file(tas_list, conf_cmip):

    # all warming levels in the same file

    fN = conf_cmip.warming_levels_folder + "warming_levels_one_ens_all.yml"
    with open(fN, "w") as fid:
        write_info(fid)
        for warming_level in WARMING_LEVELS:
            string = warming_level_years(tas_list, warming_level)
            fid.write(string)

    # one file for all warming levels

    for warming_level in WARMING_LEVELS:

        fN = conf_cmip.warming_levels_folder
        fN += "warming_levels_one_ens_{}.yml".format(
            warming_level_to_str(warming_level)
        )

        with open(fN, "w") as fid:
            write_info(fid)
            string = warming_level_years(
                tas_list, warming_level, print_warming_level=False
            )
            fid.write(string)


if __name__ == "__main__":

    WARMING_LEVELS = [1.5, 2.0, 3.0, 4.0]

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
