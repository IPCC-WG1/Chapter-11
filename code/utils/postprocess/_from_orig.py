from .. import transform
from .common import ProcessorFromOrig


class GlobalMeanFromOrig(ProcessorFromOrig):
    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        return transform.Globmean(meta["varn"])(ds)


class NoTransformFromOrig(ProcessorFromOrig):
    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        return transform.NoTransform(meta["varn"])(ds)


class SelectGridpointFromOrig(ProcessorFromOrig):
    def transform(self, coords):

        self.coords = coords
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        return transform.SelectGridpoint(meta["varn"])(ds, **self.coords)


class SelectRegionFromOrig(ProcessorFromOrig):
    def transform(self, coords):

        self.coords = coords
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        return transform.SelectRegion(meta["varn"])(ds, **self.coords)


class CDDFromOrig(ProcessorFromOrig):

    _postprocess_name = "CDD"

    # set varn="pr" as default?

    def transform(self, freq="A"):

        self.freq = freq
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        return transform.CDD(meta["varn"], freq=self.freq)(ds)


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
    def transform(self, how):

        self.how = how
        super().transform()

    def _transform(self, **meta):

        transform_func = transform.ResampleAnnual(var=meta["varn"], how=self.how)

        ds = self.conf_cmip.load_orig(**meta)
        return transform_func(ds)


class ResampleMonthlyFromOrig(ProcessorFromOrig):
    def transform(self, how):

        self.how = how
        super().transform()

    def _transform(self, **meta):

        transform_func = transform.ResampleMonthly(var=meta["varn"], how=self.how)

        ds = self.conf_cmip.load_orig(**meta)
        return transform_func(ds)


class ResampleAnnualQuantileFromOrig(ProcessorFromOrig):
    def transform(self, q):

        self.q = q
        super().transform()

    def _transform(self, **meta):

        transform_func = transform.ResampleAnnual(
            var=meta["varn"], how="quantile", q=self.q
        )

        ds = self.conf_cmip.load_orig(**meta)
        return transform_func(ds)


class RegionAverageFromOrig(ProcessorFromOrig):
    def transform(self, regions):

        self.regions = regions
        super().transform()

    def _transform(self, **meta):

        transform_func = transform.RegionAverage(var=meta["varn"], regions=self.regions)

        ds = self.conf_cmip.load_orig(**meta)
        return transform_func(ds)
