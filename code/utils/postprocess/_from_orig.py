from .. import transform
from .common import ProcessorFromOrig


class GlobalMeanFromOrig(ProcessorFromOrig):
    """calculate global average of CMIP data"""

    def transform(self, lat_weights=None, weights=None):
        """calculate global average for all selected CMIP simulations

        Parameters
        ----------
        lat_weights : str, default: None
            Name of the latitude weights, e.g. "areacella". Uses cosine weights
            if None or if they are unavailable (and the coordinates are not 2D).
        weights : str, default: None
            Name of the fractional weights (e.g. "land") to apply.
        """

        self.lat_weights = lat_weights
        self.weights = weights

        # mask_out makes no sense for the global average - use ``weights``
        self.mask_out = None

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)

        # get lat_weights * weights (e.g. to compute land-only average)
        weights = self.get_area_weights(self.lat_weights, self.weights, meta, ds)
        if not len(weights):
            return []

        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.Globmean(meta["varn"], weights=weights, mask=mask)(ds.load())


class NoTransformFromOrig(ProcessorFromOrig):
    """no transformation of CMIP data (just copy it over to the archive)"""

    def transform(self, mask_out=None):
        """apply _no_ transformation for all selected CMIP simulations

        Parameters
        ----------
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.NoTransform(meta["varn"], mask=mask)(ds)


class SelectGridpointFromOrig(ProcessorFromOrig):
    """select a set of coords of CMIP data"""

    def transform(self, coords, mask_out=None):
        """index a set of coords for all selected CMIP simulations

        Parameters
        ----------
        coords : dict of indexers
            A dict with keys matching dimensions and values given by scalars.
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

        self.coords = coords
        self.mask_out = mask_out
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.SelectGridpoint(meta["varn"], mask=mask)(ds, **self.coords)


class SelectRegionFromOrig(ProcessorFromOrig):
    """select a rectangular region of CMIP data"""

    def transform(self, coords, mask_out=None):
        """index a rectangular region (slice) for all selected CMIP simulations

        Parameters
        ----------
        coords : dict of indexers
            A dict with keys matching dimensions and values given by slices.
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

        self.coords = coords
        self.mask_out = mask_out
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.SelectRegion(meta["varn"], mask=mask, **self.coords)(ds)


class CDDFromOrig(ProcessorFromOrig):
    """calculate consecutive dry days of CMIP data"""

    _postprocess_name = "CDD"

    # set varn="pr" as default?

    def transform(self, freq="A", mask_out=None):
        """calculate consecutive dry days for all selected CMIP simulations

        Parameters
        ----------
        freq : str, default: "A"
            Resampling frequency (offset alias).
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

        self.freq = freq
        self.mask_out = mask_out
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.CDD(meta["varn"], freq=self.freq, mask=mask)(ds)


class RxNdayFromOrig(ProcessorFromOrig):
    """calculate max n-day precipitation amount of CMIP data"""

    def transform(self, window):
        """calculate max n-day precipitation amount for all selected CMIP simulations

        Parameters
        ----------
        window : int
            Moving window size.
        """

        self.window = window
        super().transform()
        # set varn="pr" as default?

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        return transform.RollingResampleAnnual(
            var=meta["varn"], window=self.window, how_rolling="sum", how="max"
        )(ds)


class TxDaysAboveFromOrig(ProcessorFromOrig):
    """calculate number of days above threshold for daily maximum temp. of CMIP data"""

    postprocess_name = "tx_days_above_35"
    # set varn="tasmax" as default?

    def transform(self, thresh="35.0 degC", freq="A"):
        """calculate consecutive dry days for all selected CMIP simulations

        Parameters
        ----------
        thresh : str, default: '35.0 degC'
            Threshold temperature on which to base evaluation [℃] or [K].
        freq : str, default: "A"
            Resampling frequency (offset alias).
        """

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
    """resample to annual of CMIP data"""

    def transform(self, how, mask_out=None):
        """downsample data to annual resolution for all selected CMIP simulations

        Parameters
        ----------
        how : str
            Used for downsampling. Must be a valid aggregation operation, e.g. "mean".
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

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
    """resample to monthly of CMIP data"""

    def transform(self, how, mask_out=None):
        """downsample data to monthly resolution for all selected CMIP simulations

        Parameters
        ----------
        how : str
            Used for downsampling. Must be a valid aggregation operation, e.g. "mean".
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

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
    """resample to annual quantiles of CMIP data"""

    def transform(self, q, mask_out=None):
        """downsample data to annual quantiles for all selected CMIP simulations

        Parameters
        ----------
        q : float or array-like of float
            Quantile to compute, which must be between 0 and 1 inclusive.
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

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
    """calculate regional average of CMIP data"""

    def transform(
        self, regions, lat_weights=None, weights=None, mask_out=None, land_only=True
    ):
        """calculate global average for all selected CMIP simulations

        Parameters
        ----------
        regions : regionmask.Regions
            Regions to take the average over.
        lat_weights : str, default: None
            Name of the latitude weights, e.g. "areacella". Uses cosine weights
            if None or if they are unavailable (and the coordinates are not 2D).
        weights : str, default: None
            Name of the fractional weights (e.g. "land") to apply.
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        land_only : bool, default: True
            Whether to mask out ocean points before calculating regional means.
        """

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
    """calculate the calendar months that have the consecutive max/min  of CMIP data"""

    def transform(self, how, beg, end, dim="time", mask_out=None):
        """
        calculate climatological min/ max of consecutive months (i.e. rolling with
        wrap-around for all selected CMIP simulations

        Parameters
        ----------
        how : "min" | "max"
            Which reduction to apply, e.g. "mean", "std".
        beg : int
            Start year of the climatology period
        end : int
            End year of the climatology period
        dim : str
            Name of the time dimension.
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

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
