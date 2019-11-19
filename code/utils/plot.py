import xarray as xr
import mplotutils as mpu
import cartopy.crs as ccrs
import matplotlib.pyplot as plt


def at_warming_level(
    at_warming, unit, title, levels, average, mask_ocean=False, **kwargs
):

    # determine function for averaging
    if average == "mean":
        ave_fnc = xr.DataArray.mean
    elif average == "median":
        ave_fnc = xr.DataArray.median

        title += " – median"
    else:
        raise "Not implemented"

    f, axes = plt.subplots(2, 2, subplot_kw=dict(projection=ccrs.Robinson()))

    axes = axes.flatten()

    opt = dict(
        transform=ccrs.PlateCarree(),
        add_colorbar=False,
        rasterized=True,
        extend="both",
        levels=levels,
        **kwargs
    )

    for i in range(4):

        ax = axes[i]
        d = ave_fnc(at_warming[i], "ens")
        h = d.plot(ax=ax, **opt)

        # add_sig_hatch_sign(ax, at_warming[i], thresh)

    for ax in axes:
        ax.coastlines(zorder=4, lw=0.5)
        ax.set_global()

    if mask_ocean:
        for ax in axes:
            raise NotImplementedError()

    cbar = mpu.colorbar(
        h, axes[2], axes[3], aspect=25, shrink=0.2, orientation="horizontal"
    )
    cbar.set_label(unit)

    titles = [
        "Tglob anomaly +1.5 °C",
        "Tglob anomaly +2.0 °C",
        "Tglob anomaly +3.0 °C",
        "Tglob anomaly +4.0 °C",
    ]

    for i, ax in enumerate(axes[:4]):
        ax.set_title(titles[i], fontsize=8)

    f.suptitle(title, fontsize=10, y=0.96)

    plt.subplots_adjust(wspace=0.1, left=0.025, right=1 - 0.025, bottom=0.2, top=0.875)

    mpu.set_map_layout(axes)

    f.canvas.draw()
