from .. import transform
from .common import ProcessorFromPost


class RegridFromPost(ProcessorFromPost):

    # need to overwrite transform as we regrid using cdo

    def transform(
        self,
        target_grid="g025",
        overwrite=False,
        method="con2",
    ):

        for fN_in, meta in self._yield_transform():

            fN_out = self.fN_out(**meta)
            transform.regrid_cdo(fN_in, fN_out, target_grid=target_grid, method=method)


class RegionAverageFromPost(ProcessorFromPost):
    def transform(self, regions, lat_weights=None, weights=None, land_only=True):

        self.regions = regions
        self.lat_weights = lat_weights
        self.weights = weights
        self.land_only = land_only

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_post(**meta)

        # get latitude weights
        lat_weights = self.get_lat_weights(self.lat_weights, meta, ds)
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
    def transform(
        self,
        period=20,
        min_length=500,
        cut_start=100,
        deg=2,
    ):

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
    def transform(self, how, invalidate_beg_end, from_concat):
        """
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
