import xarray as xr


class _cmip_conf:
    """docstring for cmip5_Conf"""

    def __init__(self):
        raise ValueError("Use 'conf.cmip5' of 'conf.cmip6' instead")

    @property
    def files_orig(self):
        return self._files_orig

    @property
    def files_post(self):
        return self._files_post

    @property
    def hist_period(self):
        return self._hist_period

    @property
    def proj_period(self):
        return self._proj_period

    @property
    def scenarios(self):
        return self._scenarios

    @property
    def scenarios_all(self):
        return self._scenarios_all

    @property
    def scenarios_incl_hist(self):
        return self._scenarios + ["historical"]

    @property
    def scenarios_all_incl_hist(self):
        return self._scenarios_all + ["historical"]












def _combiner(self, raw_func, scen, **metadata):
    """combine historical simulation and projection

    Parameters
    ----------
    raw_func : function
        Function that reads data for a given scenario
    scen : string
        rcp scenario, cannot be 'historical'

    Other Arguments
    ---------------
    Other arguments that raw_func needs, e.g. 'var', 'model', ...


    """

    raw_func_name = raw_func.__name__

    msg = f"Use '{raw_func_name}' to load historical scen"
    assert scen != "historical", msg

    varn = metadata.get("varn", "var: unkown")
    model = metadata.get("model", "model: unkown")
    ens = metadata.get("ens", "ens: unkown")

    # ====
    # load historical
    hist = raw_func("historical", **metadata)
    if not len(hist):
        msg = f"-- no 'historical' for '{varn}' '{model}' '{ens}'"
        return []

    hist = hist.sel(time=self.hist_period)

    # ====
    # load projection
    proj = raw_func(scen, **metadata)
    if not len(proj):
        msg = f"-- no 'historical' for '{varn}' '{model}' '{ens}'"
        return []

    proj = proj.sel(time=self.proj_period)

    # combine
    return xr.concat([hist, proj], dim="time")
