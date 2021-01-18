from .. import transform
from .. import xarray_utils as xru
from .common import ProcessorFromOrig  # , ProcessorFromPost


class TosGlobmeanMaskIceAnyFromOrig(ProcessorFromOrig):
    def transform(self, lat_weights=None):
        self.lat_weights = lat_weights
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)

        weights = self.get_lat_weights(self.lat_weights, meta, ds)
        if weights is None or not len(weights):
            return []

        meta_ice_any = meta.copy()
        [meta_ice_any.pop(x) for x in ["varn", "table", "exp"]]

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
            **meta_ice_any
        )

        if not len(mask):
            return []

        mask = mask[varn].any("time")
        mask = ~mask

        if not xru.alignable(ds, mask):
            return []

        return transform.Globmean(meta["varn"], weights=weights, mask=mask)(ds)


class VPDFromOrig(ProcessorFromOrig):
    def transform(self, varn_rh="hurs", mask_out=None):
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
    def transform(self, quantile=0.1, beg=1850, end=1900, dim="time", mask_out=None):

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
