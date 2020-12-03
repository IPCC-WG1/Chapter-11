import glob

from ._fixes_common import (
    _corresponds_to,
    _remove_matching_fN,
    _remove_non_matching_fN,
    fixes_common,
)


def cmip5_files(folder_in):
    def _inner(metadata):

        # fix before glob

        # skip due to mess in files folder
        if _corresponds_to(
            metadata,
            exp="rcp85",
            table="day",
            varn=["tasmax", "tasmin", "pr"],
            model="HadGEM2-ES",
            ens="r1i1p1",
        ):
            return None

        # skip: something goes wrong in California ~ 2 °C temperature jump downwards
        # e.g. 29.36°N 253°E in 1960
        if _corresponds_to(metadata, model=["MIROC5", "MIROC-ESM-CHEM", "MIROC-ESM"]):
            return None

        # skip: uses mixed Gregorian/Julian calendar but goes over 1582-10-15
        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn=["pr", "tasmin"],
            model="CMCC-CM",
            ens="r1i1p1",
        ):
            return None

        # time is not monotonic in file
        if _corresponds_to(
            metadata,
            exp="historical",
            table="Amon",
            varn="tas",
            model="EC-EARTH",
            ens=["r7i1p1", "r11i1p1", "r13i1p1", "r14i1p1"],
        ):
            return None

        # at least one year of data is missing
        if _corresponds_to(
            metadata,
            exp="historical",
            table="day",
            varn="pr",
            model="CESM1-CAM5",
            ens="r1i1p1",
        ):
            return None

        # =========================================================================

        # get the files in the directory
        fNs_in = sorted(glob.glob(folder_in))

        # =========================================================================

        # fix after glob

        # some time period exists twice
        if _corresponds_to(
            metadata,
            exp="rcp45",
            table="day",
            varn="tasmax",
            model="CMCC-CMS",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in, "tasmax_day_CMCC-CMS_rcp45_r1i1p1_20060101-20090930.nc"
            )

        # some time period exists twice
        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn=["pr", "tasmin"],
            model="CMCC-CMS",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "_day_CMCC-CMS_piControl_r1i1p1_38200101-38291231.nc",
                "_day_CMCC-CMS_piControl_r1i1p1_38300101-38391231.nc",
            )

        # some time periods after 2000 exists more than once
        if _corresponds_to(
            metadata,
            exp="historical",
            table="day",
            varn=["pr", "tasmin"],
            model="GISS-E2-H",
            ens="r6i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "_day_GISS-E2-H_historical_r6i1p1_20010101-20051231.nc",
                "_day_GISS-E2-H_historical_r6i1p1_20000101-20121231.nc",
            )

        # all after 2100 have a wrong time
        if _corresponds_to(
            metadata,
            exp="rcp45",
            table="day",
            varn="tasmax",
            model="GFDL-CM3",
            ens="r1i1p1",
        ):
            fNs_in = fNs_in[:19]

        # some time period exists twice
        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn=["tasmax", "tasmin", "pr"],
            model="HadGEM2-ES",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in, "_day_HadGEM2-ES_piControl_r1i1p1_20981201-21081130.nc"
            )

        # there is a problem at the end of the 21st century
        if _corresponds_to(
            metadata,
            exp="rcp45",
            table="day",
            varn=["tasmin", "tasmax"],
            model="HadGEM2-ES",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "day_HadGEM2-ES_rcp45_r1i1p1_20991201-21091130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21091201-21191130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21191201-21291130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21291201-21391130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21391201-21491130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21491201-21591130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21591201-21691130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21691201-21791130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21791201-21891130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21891201-21991130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_21991201-22091130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22091201-22191130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22191201-22291130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22291201-22391130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22391201-22491130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22491201-22591130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22591201-22691130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22691201-22791130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22791201-22891130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22891201-22991130.nc",
                "day_HadGEM2-ES_rcp45_r1i1p1_22991201-22991230.nc",
            )

        # some time period exists twice
        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn="tasmax",
            model="IPSL-CM5B-LR",
            ens="r1i1p1",
        ):
            fNs_in = [fNs_in[0], fNs_in[4]]

        # some time period exists twice
        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn="pr",
            model="IPSL-CM5B-LR",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "pr_day_IPSL-CM5B-LR_piControl_r1i1p1_18300101-20291231.nc",
                "pr_day_IPSL-CM5B-LR_piControl_r1i1p1_20300101-21291231.nc",
                "pr_day_IPSL-CM5B-LR_piControl_r1i1p1_20800101-21291231.nc",
            )
        # some time period exists twice

        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn="tasmin",
            model="IPSL-CM5B-LR",
            ens="r1i1p1",
        ):
            fNs_in = _remove_non_matching_fN(
                fNs_in,
                "tasmin_day_IPSL-CM5B-LR_piControl_r1i1p1_18300101-20291231.nc",
                "tasmin_day_IPSL-CM5B-LR_piControl_r1i1p1_20300101-21291231.nc",
            )

        if _corresponds_to(
            metadata,
            exp="rcp85",
            table="Amon",
            varn="tas",
            model="EC-EARTH",
            ens="r6i1p1",
        ):
            fNs_in = _remove_non_matching_fN(
                fNs_in,
                "tas_Amon_EC-EARTH_rcp85_r6i1p1_200601-205012.nc",
                "tas_Amon_EC-EARTH_rcp85_r6i1p1_205101-210012.nc",
            )

        # the grid changes after 2100
        if _corresponds_to(
            metadata,
            exp=["rcp45", "rcp60", "rcp85"],
            table="day",
            varn="tas",
            model="CCSM4",
            ens="r1i1p1",
        ):
            fNs_in = _remove_non_matching_fN(
                fNs_in,
                "_r1i1p1_21010101-21241231.nc",
                "_r1i1p1_21250101-21491231.nc",
                "_r1i1p1_21500101-21741231.nc",
                "_r1i1p1_21750101-21991231.nc",
                "_r1i1p1_22000101-22241231.nc",
                "_r1i1p1_22250101-22491231.nc",
                "_r1i1p1_22500101-22741231.nc",
                "_r1i1p1_22750101-22991231.nc",
            )

        if _corresponds_to(
            metadata,
            exp="rcp85",
            table="Amon",
            varn="tas",
            model="EC-EARTH",
            ens="r11i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_200601-200912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_201001-201912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_202001-202912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_203001-203912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_204001-204912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_205001-205912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_206001-206912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_207001-207912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_208001-208912.nc",
                "tas_Amon_EC-EARTH_rcp85_r11i1p1_209001-209912.nc",
            )

        if _corresponds_to(
            metadata,
            exp="rcp85",
            table="day",
            varn="tasmin",
            model="EC-EARTH",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20060101-20091231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20100101-20191231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20200101-20291231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20300101-20391231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20400101-20491231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20500101-20591231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20600101-20691231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20700101-20791231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20800101-20891231.nc",
                "tasmin_day_EC-EARTH_rcp85_r1i1p1_20900101-20991231.nc",
            )

        if _corresponds_to(
            metadata,
            exp="rcp45",
            table="day",
            varn="tasmin",
            model="EC-EARTH",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20060101-20091231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20100101-20191231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20200101-20291231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20300101-20391231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20400101-20491231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20500101-20591231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20600101-20691231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20700101-20791231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20800101-20891231.nc",
                "tasmin_day_EC-EARTH_rcp45_r1i1p1_20900101-20991231.nc",
            )

        if _corresponds_to(
            metadata,
            exp="historical",
            table="day",
            varn="tasmin",
            model="EC-EARTH",
            ens="r1i1p1",
        ):
            fNs_in = _remove_matching_fN(
                fNs_in,
                "tasmin_day_EC-EARTH_historical_r1i1p1_18500101-18591231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_18600101-18691231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_18700101-18791231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_18800101-18891231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_18900101-18991231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19000101-19091231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19100101-19191231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19200101-19291231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19300101-19391231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19400101-19491231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19500101-19591231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19600101-19691231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19700101-19791231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19800101-19891231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_19900101-19991231.nc",
                "tasmin_day_EC-EARTH_historical_r1i1p1_20000101-20051231.nc",
            )

        return fNs_in

    return _inner


def cmip5_data(ds, metadata):

    ds = fixes_common(ds)

    # the year 1941 is wrong
    if _corresponds_to(
        metadata,
        exp="historical",
        table="day",
        varn="tasmax",
        model="CESM1-CAM5",
        ens="r1i1p1",
    ):
        ds = ds.where(ds > -1000)

    return ds
