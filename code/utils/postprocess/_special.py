from .. import transform
from .. import xarray_utils as xru
from .common import ProcessorFromOrig, ProcessorFromPost


class TosGlobmeanMaskIceAnyFromOrig(ProcessorFromOrig):
    def transform(self, fx_weights=None):
        self.fx_weights = fx_weights
        super().transform()

    def _transform(self, **meta):

        ds = self.conf_cmip.load_orig(**meta)

        weights = self.get_lat_weights(self.fx_weights, meta, ds)
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
