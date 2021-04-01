from .. import transform
from .common import ProcessorFromOrig


class GlobalMeanFromOrig(ProcessorFromOrig):
    def transform(self, lat_weights=None, weights=None, mask_out=None):
        self.lat_weights = lat_weights
        self.weights = weights
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)

        weights = self.get_area_weights(self.lat_weights, self.weights, meta, ds)
        if not len(weights):
            return []

        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.Globmean(meta["varn"], weights=weights, mask=mask)(ds.load())


class NoTransformFromOrig(ProcessorFromOrig):
    def transform(self, lat_weights=None, mask_out=None):
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.NoTransform(meta["varn"], mask=mask)(ds)


class SelectGridpointFromOrig(ProcessorFromOrig):
    def transform(self, coords, mask_out=None):

        self.coords = coords
        self.mask_out = mask_out
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.SelectGridpoint(meta["varn"], mask=mask)(ds, **self.coords)


class SelectRegionFromOrig(ProcessorFromOrig):
    def transform(self, coords, mask_out=None):

        self.coords = coords
        self.mask_out = mask_out
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.SelectRegion(meta["varn"], mask=mask)(ds, **self.coords)


class CDDFromOrig(ProcessorFromOrig):

    _postprocess_name = "CDD"

    # set varn="pr" as default?

    def transform(self, freq="A", mask_out=None):

        self.freq = freq
        self.mask_out = mask_out
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.CDD(meta["varn"], freq=self.freq, mask=mask)(ds)


class RxNdayFromOrig(ProcessorFromOrig):
    def transform(self, window):

        self.window = window
        super().transform()
        # set varn="pr" as default?

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        return transform.RollingResampleAnnual(
            var=meta["varn"], window=self.window, how_rolling="sum", how="max"
        )(ds)


class TxDaysAboveFromOrig(ProcessorFromOrig):

    postprocess_name = "tx_days_above_35"

    # set varn="tasmax" as default?

    def transform(self, thresh="35.0 degC", freq="A"):

        self.freq = freq
        self.thresh = thresh
        super().transform()

    def _transform(self, **meta):

        transform_func = transform.TX_Days_Above(
            var=meta["varn"], freq=self.freq, thresh=self.thresh
        )

        ds = self.conf_cmip.load_orig(**meta)
        return transform_func(ds)


class ResampleAnnualFromOrig(ProcessorFromOrig):
    def transform(self, how, mask_out=None):

        self.how = how
        self.mask_out = mask_out
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        transform_func = transform.ResampleAnnual(
            var=meta["varn"], how=self.how, mask=mask
        )
        return transform_func(ds)


class ResampleMonthlyFromOrig(ProcessorFromOrig):
    def transform(self, how, mask_out=None):

        self.how = how
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        transform_func = transform.ResampleMonthly(
            var=meta["varn"], how=self.how, mask=mask
        )
        return transform_func(ds)


class ResampleAnnualQuantileFromOrig(ProcessorFromOrig):
    def transform(self, q, mask_out=None):

        self.q = q
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        transform_func = transform.ResampleAnnual(
            var=meta["varn"], how="quantile", q=self.q, mask=mask
        )
        return transform_func(ds)


class RegionAverageFromOrig(ProcessorFromOrig):
    def transform(
        self, regions, lat_weights=None, weights=None, mask_out=None, land_only=True
    ):

        self.regions = regions
        self.lat_weights = lat_weights
        self.weights = weights
        self.mask_out = mask_out
        self.land_only = land_only

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)

        weights = self.get_area_weights(self.lat_weights, self.weights, meta, ds)
        if not len(weights):
            return []

        mask = self.get_masks(self.mask_out, meta, ds)

        transform_func = transform.RegionAverage(
            var=meta["varn"],
            regions=self.regions,
            weights=weights,
            landmask=mask,
            land_only=self.land_only,
        )
        return transform_func(ds)


class ConsecutiveMonthsClim(ProcessorFromOrig):
    def transform(self, how, beg, end, dim="time", mask_out=None):

        beg, end = str(beg), str(end)

        self.how = how
        self.clim = slice(beg, end)
        self.dim = dim
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        transform_func = transform.ConsecutiveMonthsClim(
            var=meta["varn"], how=self.how, clim=self.clim, dim=self.dim, mask=mask
        )

        return transform_func(ds)
