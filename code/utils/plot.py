import cartopy.crs as ccrs
import matplotlib as mpl
import matplotlib.pyplot as plt
import mplotutils as mpu
import xarray as xr

from . import computation


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

    for i in range(3):

        ax = axes[i * 3 + 1]
        d = ave_fnc(at_warming_c6[i], "ens")
        h = d.plot(ax=ax, **opt)

    cbar = mpu.colorbar(
        h, axes[-3], axes[-2], aspect=25, shrink=0.2, orientation="horizontal"
    )
    cbar.set_label(unit)
    cbar.ax.tick_params(labelsize=10)

    # =====

    # diference plots

    opt.update(**opt_diff)

    for i in range(3):
        d1 = at_warming_c6[i]
        d2 = at_warming_c5[i]

        ax = axes[i * 3 + 2]
        d = ave_fnc(d1, "ens") - ave_fnc(d2, "ens")
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

    axes[0].set_title("CMIP5", fontsize=11)
    axes[1].set_title("CMIP6", fontsize=11)

    axes[2].set_title("CMIP6 - CMIP5", fontsize=11)

    f.suptitle(title, fontsize=11, y=0.975)

    plt.subplots_adjust(
        wspace=0.075, left=0.05, right=1 - 0.025, bottom=0.175, top=0.875
    )

    mpu.set_map_layout(axes)

    f.canvas.draw()

    return cbar
