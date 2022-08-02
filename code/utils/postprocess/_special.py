from .. import transform
from .. import xarray_utils as xru
from .common import ProcessorFromOrig  # , ProcessorFromPost


class TosGlobmeanMaskIceAnyFromOrig(ProcessorFromOrig):
    """calculate global average tos for alltime ice free cells of CMIP data"""

    def transform(self, lat_weights=None):
        """calculate global average tos for alltime ice free cells for all selected CMIP
        simulations

        Parameters
        ----------
        lat_weights : str, default: None
            Name of the latitude weights, e.g. "areacella". Uses cosine weights
            if None or if they are unavailable (and the coordinates are not 2D).
        """

        self.lat_weights = lat_weights
        super().transform()

    def _transform(self, **meta):

        # load tos data
        ds = self.conf_cmip.load_orig(**meta)

        weights = self.get_lat_weights(self.lat_weights, meta, ds)
        if weights is None or not len(weights):
            return []

        # read sea ice data

        # create a meta dict for ice (sic/ siconc)
        meta_ice = meta.copy()
        [meta_ice.pop(x) for x in ["varn", "table", "exp"]]

        # TODO: better solution for different names in cmip5/ cmip6
        if self.conf_cmip.cmip == "cmip5":
            varn = "sic"
            table = "OImon"
        else:
            varn = "siconc"
            table = "SImon"

        # load the siconc.resample(time="A").any() data
        mask = self.conf_cmip.load_post(
            varn=varn,
            postprocess="any_annual",
            table=table,
            exp="historical",
            **meta_ice
        )

        if not len(mask):
            return []

        mask = mask[varn].any("time")
        mask = ~mask

        if not xru.alignable(ds, mask):
            return []

        return transform.Globmean(meta["varn"], weights=weights, mask=mask)(ds)


# ======================================================================================
# NOT USED


class VPDFromOrig(ProcessorFromOrig):
    """calculate vapour pressure deficit (VPD) of CMIP data"""

    def transform(self, varn_rh="hurs", mask_out=None):
        """calculate VPD for all selected CMIP simulations

        Parameters
        ----------
        varn_rh : str, default: "hurs"
            Name of the relative humidity variable.
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).
        """

        self.mask_out = mask_out
        self.varn_rh = varn_rh

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)

        meta_rh = meta.copy()
        meta_rh["varn"] = self.varn_rh

        rh = self.conf_cmip.load_orig(**meta_rh)[self.varn_rh]

        if not len(rh):
            return []

        mask = self.get_masks(self.mask_out, meta, ds)
        return transform.VPD(meta["varn"], rh=rh, mask=mask)(ds)


class SMDryDaysClimZhangFromOrig(ProcessorFromOrig):
    """calculate dry days climatology with bootstraping of CMIP data"""

    def transform(self, quantile=0.1, beg=1850, end=1900, dim="time", mask_out=None):
        """calc climatology of SM dry days with bootstrapping after Zhang 2005 for all
        selected CMIP simulations

        Parameters
        ----------
        quantile : float, default: 0.1
            Quantile in range 0..1
        beg : int, default: 1850
            Start of climatology period.
        end : int, default: 1900
            End of climatology period.
        mask_out : str, list of str, or None, default: None
            Name(s) of the mask(s) to apply (sets values to NaN).

        References
        ----------
        https://doi.org/10.1175/JCLI3366.1


        """
        self.quantile = quantile
        self.beg = beg
        self.end = end
        self.dim = dim
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)
        transform_func = transform.SM_dry_days_clim_Zhang(
            var=meta["varn"],
            quantile=self.quantile,
            beg=self.beg,
            end=self.end,
            dim=self.dim,
            mask=mask,
        )
        return transform_func(ds)


class SMDryDaysZhangFromOrig(ProcessorFromOrig):
    def transform(
        self, postprocess_name_clim, is_pic, dim="time", freq="A", mask_out=None
    ):

        self.postprocess_name_clim = postprocess_name_clim
        self.is_pic = is_pic
        self.dim = dim
        self.freq = freq
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)

        meta_thresh = meta.copy()
        meta_thresh["exp"] = "historical"
        threshold = self.conf_cmip.load_post(
            postprocess=self.postprocess_name_clim, **meta_thresh
        )

        transform_func = transform.SM_dry_days_Zhang(
            var=meta["varn"],
            threshold=threshold,
            is_pic=self.is_pic,
            dim=self.dim,
            freq=self.freq,
            mask=mask,
        )

        return transform_func(ds)


class SMDryDaysIntensityZhangFromOrig(ProcessorFromOrig):
    def transform(
        self, postprocess_name_clim, is_pic, dim="time", freq="A", mask_out=None
    ):

        self.postprocess_name_clim = postprocess_name_clim
        self.is_pic = is_pic
        self.dim = dim
        self.freq = freq
        self.mask_out = mask_out

        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)
        mask = self.get_masks(self.mask_out, meta, ds)

        meta_thresh = meta.copy()
        meta_thresh["exp"] = "historical"
        threshold = self.conf_cmip.load_post(
            postprocess=self.postprocess_name_clim, **meta_thresh
        )

        transform_func = transform.SM_dry_days_Intensity_Zhang(
            var=meta["varn"],
            threshold=threshold,
            is_pic=self.is_pic,
            dim=self.dim,
            freq=self.freq,
            mask=mask,
        )

        return transform_func(ds)
