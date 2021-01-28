import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib as mpl
import matplotlib.pyplot as plt
import mplotutils as mpu
import numpy as np
import xarray as xr

from . import computation


def map_subplots(
    nrows=1,
    ncols=1,
    *,
    projection=ccrs.Robinson(),
    squeeze=True,
    subplot_kw=None,
    gridspec_kw=None,
    **fig_kw,
):

    if subplot_kw is None:
        subplot_kw = dict()

    subplot_kw["projection"] = projection

    f, axes = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        squeeze=squeeze,
        subplot_kw=subplot_kw,
        gridspec_kw=gridspec_kw,
        **fig_kw,
    )

    if isinstance(axes, np.ndarray):
        axes = axes.flatten()

    return f, axes


def one_map_flat(
    da,
    ax,
    levels=None,
    mask_ocean=False,
    ocean_kws=None,
    skipna=None,
    add_coastlines=True,
    coastline_kws=None,
    **kwargs,
):
    opt = dict(
        transform=ccrs.PlateCarree(),
        add_colorbar=False,
        rasterized=True,
        extend="both",
        levels=levels,
    )
    # allow to override the defaults
    opt.update(kwargs)

    if ocean_kws is None:
        ocean_kws = dict(color="w", zorder=1.1)

    if coastline_kws is None:
        coastline_kws = dict(color="0.1", lw=1, zorder=1.2)

    if mask_ocean:
        NEF = cfeature.NaturalEarthFeature
        OCEAN = NEF("physical", "ocean", "110m")

    h = da.plot(ax=ax, **opt)

    if mask_ocean:
        ax.add_feature(OCEAN, **ocean_kws)

    if add_coastlines:
        ax.coastlines(**coastline_kws)

    s = ax.spines["geo"]
    s.set_lw(0.5)
    s.set_color("0.5")

    ax.set_global()

    return h


def one_map(
    da,
    ax,
    average,
    dim="mod_ens",
    levels=None,
    mask_ocean=False,
    ocean_kws=None,
    skipna=None,
    add_coastlines=True,
    coastline_kws=None,
    **kwargs,
):

    func = getattr(da, average)
    d = func(dim, skipna=skipna)

    n = len(da[dim])
    ax.text(1, 1, f"{n}", va="top", ha="right", transform=ax.transAxes, fontsize=9)

    h = one_map_flat(
        d,
        ax,
        levels=levels,
        mask_ocean=mask_ocean,
        ocean_kws=ocean_kws,
        skipna=skipna,
        add_coastlines=add_coastlines,
        coastline_kws=coastline_kws,
        **kwargs,
    )

    return h


def at_warming_level_one(
    at_warming_c,
    unit,
    title,
    levels,
    average,
    mask_ocean=False,
    colorbar=True,
    ocean_kws=None,
    skipna=None,
    **kwargs,
):

    if average != "mean":
        title += f" – {average}"

    f, axes = plt.subplots(1, 3, subplot_kw=dict(projection=ccrs.Robinson()))

    axes = axes.flatten()

    for i in range(3):

        h = one_map(
            at_warming_c[i],
            axes[i],
            average,
            levels=levels,
            mask_ocean=mask_ocean,
            ocean_kws=ocean_kws,
            skipna=skipna,
            **kwargs,
        )

    for ax in axes:
        ax.coastlines(zorder=4, lw=0.5)
        ax.set_global()

    if colorbar:
        cbar = mpu.colorbar(
            h,
            axes[0],
            axes[2],
            aspect=30,
            shrink=0.4,
            orientation="horizontal",
            pad=0.075,
        )
        cbar.set_label(unit, labelpad=1)
        cbar.ax.tick_params(labelsize=9, length=0)

    axes[0].set_title("At 1.5°C global warming", fontsize=9, pad=4)
    axes[1].set_title("At 2.0°C global warming", fontsize=9, pad=4)
    axes[2].set_title("At 4.0°C global warming", fontsize=9, pad=4)

    # axes[0].set_title("Tglob anomaly +1.5 °C", fontsize=9, pad=2)
    # axes[1].set_title("Tglob anomaly +2.0 °C", fontsize=9, pad=2)
    # axes[2].set_title("Tglob anomaly +4.0 °C", fontsize=9, pad=2)

    side = 0.025
    if colorbar:
        f.suptitle(title, fontsize=11, y=0.975)
        plt.subplots_adjust(
            wspace=0.075, left=side, right=1 - side, bottom=0.315, top=0.81
        )

    else:
        f.suptitle(title, fontsize=11, y=0.975)
        plt.subplots_adjust(
            wspace=0.075, left=side, right=1 - side, bottom=0.09, top=0.77
        )

    mpu.set_map_layout(axes)

    f.canvas.draw()

    if colorbar:
        return cbar


# UNUSED?


def at_warming_level(
    at_warming_c5,
    at_warming_c6,
    unit,
    title,
    levels,
    average,
    mask_ocean=False,
    **kwargs,
):

    # determine function for averaging
    if average == "mean":
        ave_fnc = xr.DataArray.mean
    elif average == "median":
        ave_fnc = xr.DataArray.median

        title += " – median"
    else:
        raise "Not implemented"

    f, axes = plt.subplots(3, 2, subplot_kw=dict(projection=ccrs.Robinson()))

    axes = axes.flatten()

    opt = dict(
        transform=ccrs.PlateCarree(),
        add_colorbar=False,
        rasterized=True,
        extend="both",
        levels=levels,
        **kwargs,
    )

    for i in range(3):

        ax = axes[i * 2]
        d = ave_fnc(at_warming_c5[i], "ens")
        h = d.plot(ax=ax, **opt)

    for i in range(3):

        ax = axes[i * 2 + 1]
        d = ave_fnc(at_warming_c6[i], "ens")
        h = d.plot(ax=ax, **opt)

        # add_sig_hatch_sign(ax, at_warming[i], thresh)

    for ax in axes:
        ax.coastlines(zorder=4, lw=0.5)
        ax.set_global()

    if mask_ocean:
        for ax in axes:
            raise NotImplementedError()

    cbar = mpu.colorbar(
        h, axes[-2], axes[-1], aspect=25, shrink=0.2, orientation="horizontal"
    )
    cbar.set_label(unit)
    cbar.ax.tick_params(labelsize=10)

    mpu.ylabel_map("Tglob anomaly +1.5 °C", ax=axes[0], fontsize=9, labelpad=6)
    mpu.ylabel_map("Tglob anomaly +2.0 °C", ax=axes[2], fontsize=9, labelpad=6)
    mpu.ylabel_map("Tglob anomaly +4.0 °C", ax=axes[4], fontsize=9, labelpad=6)

    axes[0].set_title("CMIP5", fontsize=11)
    axes[1].set_title("CMIP6", fontsize=11)

    f.suptitle(title, fontsize=11, y=0.975)

    plt.subplots_adjust(wspace=0.075, left=0.05, right=1 - 0.025, bottom=0.15, top=0.9)

    mpu.set_map_layout(axes)

    f.canvas.draw()

    return cbar


def at_warming_level_diff(
    at_warming_c5,
    at_warming_c6,
    unit,
    title,
    levels,
    average,
    name_for_mw,
    mask_ocean=False,
    projection=ccrs.Robinson(),
    opt_diff=dict(),
    signif=True,
    title_left="CMIP5",
    title_right="CMIP6",
    title_diff=None,
    **kwargs,
):

    # determine function for averaging
    if average == "mean":
        ave_fnc = xr.DataArray.mean
    elif average == "median":
        ave_fnc = xr.DataArray.median

        title += " – median"
    else:
        raise "Not implemented"

    f, axes = plt.subplots(3, 3, subplot_kw=dict(projection=projection))

    axes = axes.flatten()

    opt = dict(
        transform=ccrs.PlateCarree(),
        add_colorbar=False,
        rasterized=True,
        extend="both",
        levels=levels,
        **kwargs,
    )

    for i in range(3):

        ax = axes[i * 3]
        d = ave_fnc(at_warming_c5[i], "ens")
        h = d.plot(ax=ax, **opt)

        n = len(at_warming_c5[i].ens)
        ax.text(1, 1, f"{n}", va="top", ha="right", transform=ax.transAxes, fontsize=9)

    for i in range(3):

        ax = axes[i * 3 + 1]
        d = ave_fnc(at_warming_c6[i], "ens")
        h = d.plot(ax=ax, **opt)

        n = len(at_warming_c6[i].ens)
        ax.text(1, 1, f"{n}", va="top", ha="right", transform=ax.transAxes, fontsize=9)

    cbar = mpu.colorbar(
        h, axes[-3], axes[-2], aspect=25, shrink=0.2, orientation="horizontal"
    )
    cbar.set_label(unit)
    cbar.ax.tick_params(labelsize=10)

    # =====
    opt.pop("colors", None)

    # diference plots

    opt.update(**opt_diff)

    for i in range(3):
        d1 = at_warming_c5[i]
        d2 = at_warming_c6[i]

        ax = axes[i * 3 + 2]
        d = ave_fnc(d2, "ens") - ave_fnc(d1, "ens")
        h = d.plot(ax=ax, **opt)

        if signif:
            mpl.rcParams["hatch.linewidth"] = 0.25
            name = name_for_mw + f"_{i}"
            mw = computation.get_mannwhitney(d1, d2, name)
            mw.plot.contourf(
                levels=[0, 0.5, 1],
                colors="none",
                hatches=["", "////"],
                add_colorbar=False,
                ax=ax,
                transform=ccrs.PlateCarree(),
            )

    cbar = mpu.colorbar(
        h,
        axes[-1],
        aspect=15,
        orientation="horizontal",
        spacing="proportional",
        extend=opt.get("extend"),
    )
    cbar.set_label(unit)
    cbar.ax.tick_params(labelsize=10)

    # add_sig_hatch_sign(ax, at_warming[i], thresh)

    for ax in axes:
        ax.coastlines(zorder=4, lw=0.5)
        ax.set_global()

    if mask_ocean:
        for ax in axes:
            raise NotImplementedError()

    mpu.ylabel_map("T glob +1.5 °C", ax=axes[0], fontsize=9, labelpad=6)
    mpu.ylabel_map("T glob +2.0 °C", ax=axes[3], fontsize=9, labelpad=6)
    mpu.ylabel_map("T glob +4.0 °C", ax=axes[6], fontsize=9, labelpad=6)

    axes[0].set_title(title_left, fontsize=11)
    axes[1].set_title(title_right, fontsize=11)

    if title_diff is None:
        title_diff = f"{title_right} - {title_left}"

    axes[2].set_title(title_diff, fontsize=11)

    f.suptitle(title, fontsize=11, y=0.975)

    plt.subplots_adjust(
        wspace=0.075, left=0.05, right=1 - 0.025, bottom=0.175, top=0.875
    )

    mpu.set_map_layout(axes)

    f.canvas.draw()

    return cbar
