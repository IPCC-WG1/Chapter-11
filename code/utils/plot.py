import xarray as xr
import mplotutils as mpu
import cartopy.crs as ccrs
import matplotlib.pyplot as plt


def at_warming_level(
    at_warming_c5,
    at_warming_c6,
    unit,
    title,
    levels,
    average,
    mask_ocean=False,
    **kwargs
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
        **kwargs
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

    titles = ["Tglob anomaly +1.5 °C", "Tglob anomaly +2.0 °C", "Tglob anomaly +4.0 °C"]

    # for i, ax in enumerate(axes[:4]):
    #     ax.set_title(titles[i], fontsize=8)

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
