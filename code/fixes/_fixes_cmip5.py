import glob


from ._fixes_common import fixes_common, _maybe_remove_fN, _corresponds_to


def cmip5_files(folder_in):
    def _inner(metadata):

        # fix before glob

        if _corresponds_to(
            metadata,
            exp="rcp85",
            table="day",
            varn=["tasmax", "pr"],
            model="HadGEM2-ES",
            ens="r1i1p1",
        ):
            # skip due to mess in files folder
            return None

        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn="pr",
            model="CMCC-CM",
            ens="r1i1p1",
        ):
            # skip: uses mixed Gregorian/Julian calendar but goes over 1582-10-15
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
            fNs_in = _maybe_remove_fN(
                fNs_in, "tasmax_day_CMCC-CMS_rcp45_r1i1p1_20060101-20090930.nc"
            )

        # some time period exists twice
        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn="pr",
            model="CMCC-CMS",
            ens="r1i1p1",
        ):
            fNs_in = _maybe_remove_fN(
                fNs_in,
                "pr_day_CMCC-CMS_piControl_r1i1p1_38200101-38291231.nc",
                "pr_day_CMCC-CMS_piControl_r1i1p1_38300101-38391231.nc",
            )

        # some time periods after 2000 exists more than once
        if _corresponds_to(
            metadata,
            exp="historical",
            table="day",
            varn="pr",
            model="GISS-E2-H",
            ens="r6i1p1",
        ):
            fNs_in = _maybe_remove_fN(
                fNs_in,
                "pr_day_GISS-E2-H_historical_r6i1p1_20010101-20051231.nc",
                "pr_day_GISS-E2-H_historical_r6i1p1_20000101-20121231.nc",
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
            varn="tasmax",
            model="HadGEM2-ES",
            ens="r1i1p1",
        ):
            fNs_in = _maybe_remove_fN(
                fNs_in, "tasmax_day_HadGEM2-ES_piControl_r1i1p1_20981201-21081130.nc"
            )

        # some time period exists twice
        if _corresponds_to(
            metadata,
            exp="piControl",
            table="day",
            varn="pr",
            model="HadGEM2-ES",
            ens="r1i1p1",
        ):
            fNs_in = _maybe_remove_fN(
                fNs_in, "pr_day_HadGEM2-ES_piControl_r1i1p1_20891201-20991110.nc"
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
            fNs_in = _maybe_remove_fN(
                fNs_in,
                "pr_day_IPSL-CM5B-LR_piControl_r1i1p1_18300101-20291231.nc",
                "pr_day_IPSL-CM5B-LR_piControl_r1i1p1_20300101-21291231.nc",
                "pr_day_IPSL-CM5B-LR_piControl_r1i1p1_20800101-21291231.nc",
            )

        return fNs_in

    return _inner


def cmip5_data(ds, metadata, next_path):

    ds = fixes_common(ds, metadata)

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
