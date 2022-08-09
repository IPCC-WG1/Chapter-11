from .. import transform
from .common import ProcessorFromPost


class RegridFromPost(ProcessorFromPost):
    """regrid CMIP data using CDO"""

    def transform(
        self,
        target_grid="g025",
        overwrite=False,
        method="con2",
    ):
        """regrid all selected CMIP simulations

        Parameters
        ----------
        target_grid : str, default: "g025"
            Name of the target grid. Must be defined CDO-style in ../../../grids/.
            Creates a 2.5° x 2.5° grid per default.
        overwrite : bool, default: False
            Overwrite the target even if it already exists.
        method : str, default: "con2"
            Method to use for regridding.
        """

        # need to overwrite transform as we regrid using cdo

        for fN_in, meta in self._yield_filenames():

            fN_out = self.fN_out(**meta)
            transform.regrid_cdo(fN_in, fN_out, target_grid=target_grid, method=method)


class RegionAverageFromPost(ProcessorFromPost):
    """calculate regional average of CMIP data using regionmask"""

    def transform(self, regions, lat_weights=None, weights=None, land_only=True):
        """calculate regional average for all selected CMIP simulations

        Parameters
        ----------
        regions : regionmask.Regions
            Regions to average over. Must be a regiomask.Regions instance.
        lat_weights : default: None
            Name of the latitude weights, e.g. "areacella". Uses cosine weights
            if None or if they are unavailable.
        weights : str, default: None
            Name of the fractional weights (e.g. "land") to apply.
        land_only : bool, default: True
            If True only calculates the average over the region indicated in
            ``weights``.
        """
        self.regions = regions
        self.lat_weights = lat_weights
        self.weights = weights
        self.land_only = land_only

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_post(**meta)

        # get latitude weights
        lat_weights = self.get_lat_weights(self.lat_weights, meta, ds)

        # could not load any valid lat_weights
        if not len(lat_weights):
            return []

        # get land fraction
        landmask = self.get_weights(self.weights, meta, ds)

        transform_func = transform.RegionAverage(
            var=meta["varn"],
            regions=self.regions,
            landmask=landmask,
            land_only=self.land_only,
            weights=lat_weights,
        )

        return transform_func(ds)


class IAVFromPost(ProcessorFromPost):
    """calculate inter annual variability of CMIP data"""

    def transform(
        self,
        period=20,
        min_length=500,
        cut_start=100,
        deg=2,
    ):
        """calculate inter annual variability for all selected CMIP simulations

        Parameters
        ----------
        period : int, default: 20
            Number of years to average over before calculating the IAV.
        min_length : int, default: 500
            Minimum length of the dataset to be considered.
        cut_start : int, default: 100.
            Numbers of years that are removed at the beginning of the data.
        deg : int or None, default: 2.
            Degree of the polynom used to detrend. If None no detrending is applied.
        """

        self.period = period
        self.min_length = min_length
        self.cut_start = cut_start
        self.deg = deg

        super().transform()

    def _transform(self, **meta):

        transform_func = transform.IAV(
            meta["varn"],
            period=self.period,
            min_length=self.min_length,
            cut_start=self.cut_start,
            deg=self.deg,
        )
        ds = self.conf_cmip.load_post(**meta)
        return transform_func(ds)


class ResampleSeasonal(ProcessorFromPost):
    """calculate seasonal averages of CMIP data"""

    def transform(self, how, invalidate_beg_end, from_concat):
        """calculate inter annual variability for all selected CMIP simulations

        Parameters
        ----------
        how : int
            Number of years to average over before calculating the IAV, default: 20
        invalidate_beg_end : bool
            Whether to set the first and last timestep to NA (as it's an incomplete
            season)
        from_concat : bool
            If True uses ``conf_cmip.load_post_concat`` to load data (make sure not to
            pass "historical") else uses ``conf_cmip.load_post`` (e.g. for piControl).
        """

        self.how = how
        self.invalidate_beg_end = invalidate_beg_end
        self.from_concat = from_concat
        super().transform()

    def _transform(self, **meta):

        transform_func = transform.ResampleSeasonal(
            var=meta["varn"],
            how=self.how,
            invalidate_beg_end=self.invalidate_beg_end,
        )

        if self.from_concat:
            ds = self.conf_cmip.load_post_concat(**meta)
        else:
            ds = self.conf_cmip.load_post(**meta)

        return transform_func(ds)
