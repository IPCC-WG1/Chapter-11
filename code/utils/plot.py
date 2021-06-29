import copy
import logging
from importlib import reload

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib as mpl
import matplotlib.hatch
import matplotlib.pyplot as plt
import mplotutils as mpu
import numpy as np
import xarray as xr
from matplotlib.path import Path

from . import computation


class SmallXHatch(mpl.hatch.Shapes):
    """
    Custom hatches defined by a path drawn inside [-0.5, 0.5] square.
    Identifier 'c'.
    """

    # create a x
    p = 0.25
    verts = [
        (-p, -p),  # left, bottom
        (p, p),  # right, top
        (-p, p),  # left, top
        (p, -p),  # right, bottom
        (0.0, 0.0),  # ignored
    ]

    codes = [
        Path.MOVETO,
        Path.LINETO,
        Path.MOVETO,
        Path.LINETO,
        Path.CLOSEPOLY,
    ]

    path = Path(verts, codes)

    filled = True
    size = 1

    def __init__(self, hatch, density):
        self.num_rows = (hatch.count("c")) * density
        self.shape_vertices = self.path.vertices
        self.shape_codes = self.path.codes
        super().__init__(hatch, density)


#
#     def set_vertices_and_codes(self, vertices, codes):
#         offset = 1.0 / self.num_rows
#         shape_vertices = self.shape_vertices * offset * self.size
#         if not self.filled:
#             inner_vertices = shape_vertices[::-1] * 0.9
#         shape_codes = self.shape_codes
#         shape_size = len(shape_vertices)
#
#         cursor = 0
#         for row in range(self.num_rows + 1):
#             if row % 2 == 0:
#                 cols = np.linspace(0, 1, self.num_rows + 1)
#             else:
#                 cols = (
#                     np.linspace(offset / 2, 1 - offset / 2, self.num_rows) + offset / 2
#                 )
#             row_pos = row * offset
#             for col_pos in cols:
#                 vertices[cursor : cursor + shape_size] = shape_vertices + (
#                     col_pos,
#                     row_pos,
#                 )
#                 codes[cursor : cursor + shape_size] = shape_codes
#                 cursor += shape_size
#                 if not self.filled:
#                     vertices[cursor : cursor + shape_size] = inner_vertices + (
#                         col_pos,
#                         row_pos,
#                     )
#                     codes[cursor : cursor + shape_size] = shape_codes
#                     cursor += shape_size
#
mpl.hatch._hatch_types.append(SmallXHatch)


class TextHandler(mpl.legend_handler.HandlerBase):
    """custom legend handler for text

    Example
    -------
    ax = plt.gca()

    tx1 = ax.text(
        x=np.nan,
        y=np.nan,
        s=r"Color",    # text in the handle
        size=9,        # font size in the handle
        label="label", # label
        # box around the handle
        bbox=dict(ec="k", fc="0.9", lw=0.5, pad=0),
    )

    # need to increase the handle size
    ax.legend(
        handles=[tx1],
        handler_map={type(tx1): TextHandler()},
        fontsize=12,
        handleheight=1,
        handlelength=2,
    )

    """

    def create_artists(
        self, legend, orig_handle, xdescent, ydescent, width, height, fontsize, trans
    ):

        # create a Rectangle with the bbox props
        # because a Text artist cannot be sized
        bbox = orig_handle.get_bbox_patch()
        if bbox is not None:

            p = mpl.patches.Rectangle(
                xy=(-xdescent, -ydescent), width=width, height=height
            )
            p.update_from(bbox)
            p.set_label(orig_handle.get_label())
            p.set_transform(trans)

        # add the text to the box
        h = copy.copy(orig_handle)
        h.set_position((-xdescent + width / 2.0, -ydescent + height / 2))

        h.set_transform(trans)
        h.set_ha("center")
        h.set_va("center")
        h.set_bbox(None)  # dict(ec="0.1", fc="none"))

        fp = orig_handle.get_font_properties().copy()
        h.set_font_properties(fp)

        if bbox is None:
            return [h]
        return [p, h]


def text_legend(ax, s, label, size=8, ec="0.1", fc="none", lw=0.5):
    """add a text-legend entry

    Parameters
    ----------
    ax : Axes
        Axes to add the legend entry to
    s : str
        Text to display in the legend artist.
    label : str
        Legend entry
    size : int
        Fontsize
    ec : color, default "0.1"
        Edgecolor of the bounding box
    fc : color, default "none"
        Facecolor of the bounding box
    lw : float
        linewidth

    Returns
    -------
    legend_handle : legend_handle
    """

    # the logger prints the warning :-(
    mpl_logger = logging.getLogger("matplotlib.text")
    mpl_logger.setLevel(logging.ERROR)

    h = ax.text(
        x=np.nan,
        y=np.nan,
        s=s,
        size=size,
        label=label,
        bbox=dict(ec=ec, fc=fc, lw=lw),
    )

    return h


def hatch_map(ax, da, hatch, label, invert=False, linewidth=0.25, color="0.1"):

    legend_handle = mpl.patches.Patch(
        facecolor="none",
        ec=color,
        lw=linewidth,
        hatch=hatch,
        label=label,
    )

    mn = da.min().item()
    mx = da.max().item()

    if mx > 1 or mn < 0:
        raise ValueError("Expected da in 0..1, got {mn}..{mx}")

    # contourf has trouble if no gridcell is True
    if da.sum() == 0:
        return legend_handle

    # ~ does only work for bool dtype
    if invert:
        da = np.abs(da - 1)

    da = mpu.cyclic_dataarray(da)

    # plot "True"
    levels = [0.95, 1.05]
    hatches = [hatch, ""]

    mpl.rcParams["hatch.linewidth"] = linewidth
    mpl.rcParams["hatch.color"] = color

    # with mpl.rc_context({"hatch.linewidth": linewidth, "hatch.color": color}):
    da.plot.contourf(
        ax=ax,
        levels=levels,
        hatches=hatches,
        colors="none",
        extend="neither",
        transform=ccrs.PlateCarree(),
        add_colorbar=False,
    )

    return legend_handle


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
    add_coastlines=True,
    coastline_kws=None,
    add_land=False,
    land_kws=None,
    plotfunc="pcolormesh",
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

    if land_kws is None:
        land_kws = dict(fc="0.8", ec="none")

    if coastline_kws is None:
        coastline_kws = dict()

    if add_land:
        ax.add_feature(cfeature.LAND, **land_kws)

    if "contour" in plotfunc:
        opt.pop("rasterized", None)
        da = mpu.cyclic_dataarray(da)
        plotfunc = getattr(da.plot, plotfunc)
    elif plotfunc == "pcolormesh":
        plotfunc = getattr(da.plot, plotfunc)
    else:
        raise ValueError(f"unkown plotfunc: {plotfunc}")

    h = plotfunc(ax=ax, **opt)

    if mask_ocean:
        ocean_kws = {} if ocean_kws is None else ocean_kws
        _mask_ocean(ax, **ocean_kws)

    if add_coastlines:
        coastlines(ax, **coastline_kws)

    s = ax.spines["geo"]
    s.set_lw(0.5)
    s.set_color("0.5")

    ax.set_global()

    return h


def mask_ocean(ax, facecolor="w", zorder=1.1, lw=0, **kwargs):

    NEF = cfeature.NaturalEarthFeature
    OCEAN = NEF("physical", "ocean", "110m", )
    ax.add_feature(OCEAN, facecolor=facecolor, zorder=zorder, lw=lw, **kwargs)


# to use in one_map_flat so the name does not get shadowed
_mask_ocean = mask_ocean


def coastlines(ax, color="0.1", lw=1, zorder=1.2, **kwargs):
    ax.coastlines(color=color, lw=lw, zorder=zorder, *kwargs)


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
    hatch_simple=None,
    plotfunc="pcolormesh",
    add_n_models=True,
    **kwargs,
):

    func = getattr(da, average)
    d = func(dim, skipna=skipna)

    if add_n_models:
        n = len(da[dim])
        ax.text(1, 1, f"{n}", va="top", ha="right", transform=ax.transAxes, fontsize=9)

    h = one_map_flat(
        d,
        ax,
        levels=levels,
        mask_ocean=mask_ocean,
        ocean_kws=ocean_kws,
        add_coastlines=add_coastlines,
        coastline_kws=coastline_kws,
        plotfunc=plotfunc,
        **kwargs,
    )

    if hatch_simple is not None:
        from . import iav as iav_utils

        consistent_change = iav_utils._get_same_sign(da, hatch_simple, dim=dim)
        # we don't want to hatch Na
        all_notnull = da.notnull().all(dim)

        consistent_change = consistent_change.where(all_notnull)

        legend_handle = hatch_map(
            ax,
            consistent_change,
            8 * "/",
            label="Lack of model agreement",
            invert=True,
            linewidth=0.25,
            color="0.1",
        )
        return h, legend_handle

    return h, None


def one_map_hatched(
    da,
    iav,
    ax,
    average,
    da_abs=None,
    add_hatch=True,
    dim="mod_ens",
    levels=None,
    mask_ocean=False,
    ocean_kws=None,
    skipna=None,
    add_coastlines=True,
    coastline_kws=None,
    plotfunc="pcolormesh",
    **kwargs,
):

    from . import iav as iav_utils

    reload(iav_utils)
    reload(iav_utils)
    reload(iav_utils)

    data_iav_aligned = iav_utils.align_for_iav(iav, da, da_abs=da_abs)

    h, __ = one_map(
        data_iav_aligned.da,
        ax,
        average,
        dim=dim,
        levels=levels,
        mask_ocean=mask_ocean,
        ocean_kws=ocean_kws,
        skipna=skipna,
        add_coastlines=add_coastlines,
        coastline_kws=coastline_kws,
        plotfunc=plotfunc,
        **kwargs,
    )

    if add_hatch:
        da_abs = data_iav_aligned.da_abs
        da_abs = da_abs if da_abs is not None else data_iav_aligned.da

        legend_handle = iav_utils.add_signif_sign_hatch(
            ax, da_abs, data_iav_aligned.iav
        )
        return h, legend_handle

    return h, None


def at_warming_level_one_hatch(
    at_warming_c,
    iav,
    unit,
    title,
    levels,
    average,
    da_abs=None,
    add_hatch=True,
    mask_ocean=False,
    colorbar=True,
    ocean_kws=None,
    skipna=None,
    add_legend=False,
    plotfunc="pcolormesh",
    legend_kwargs=None,
    colorbar_kwargs=None,
    **kwargs,
):

    if len(at_warming_c) != 3 or (da_abs is not None and len(da_abs) != 3):
        raise ValueError("wrong size!")

    if average != "mean":
        title += f" – {average}"

    f, axes = plt.subplots(1, 3, subplot_kw=dict(projection=ccrs.Robinson()))

    axes = axes.flatten()
    print(kwargs)
    for i in range(3):
        da_abs_ = None if da_abs is None else da_abs[i]
        h, legend_handles = one_map_hatched(
            da=at_warming_c[i],
            iav=iav,
            ax=axes[i],
            average=average,
            da_abs=da_abs_,
            levels=levels,
            mask_ocean=mask_ocean,
            ocean_kws=ocean_kws,
            skipna=skipna,
            add_hatch=add_hatch,
            plotfunc=plotfunc,
            **kwargs,
        )

    for ax in axes:
        # ax.coastlines(zorder=4, lw=0.5)
        ax.set_global()

    if colorbar:
        colorbar_kwargs = {} if colorbar_kwargs is None else colorbar_kwargs
        
        factor = 0.66 if add_legend else 1
        ax2 = axes[1] if add_legend else axes[2]

        cbar_opt = dict(
            size=0.15,
            shrink=0.25 * factor,
            orientation="horizontal",
            pad=0.1,
        )
        
        cbar_opt.update(colorbar_kwargs)
        
        cbar = mpu.colorbar(
            h,
            axes[0],
            ax2,
            **cbar_opt
        )
        cbar.set_label(unit, labelpad=1, size=9)
        cbar.ax.tick_params(labelsize=9)  # , length=0)

    axes[0].set_title("At 1.5°C global warming", fontsize=9, pad=4)
    axes[1].set_title("At 2.0°C global warming", fontsize=9, pad=4)
    axes[2].set_title("At 4.0°C global warming", fontsize=9, pad=4)

    # axes[0].set_title("Tglob anomaly +1.5 °C", fontsize=9, pad=2)
    # axes[1].set_title("Tglob anomaly +2.0 °C", fontsize=9, pad=2)
    # axes[2].set_title("Tglob anomaly +4.0 °C", fontsize=9, pad=2)

    if add_legend and (not colorbar or not add_hatch):
        raise ValueError("Can only add legend when colorbar and add_hatch is True")

    if add_legend:
        legend_kwargs = {} if legend_kwargs is None else legend_kwargs
        legend_opt = dict(
            handlelength=2.4,
            handleheight=1.3,
            loc="lower center",
            bbox_to_anchor=(0.45, -0.6),
            fontsize=8.5,
            borderaxespad=0,
            frameon=True,
            handler_map={mpl.text.Text: TextHandler()},
            ncol=1,
        )
        
        legend_opt.update(legend_kwargs)
        
        axes[2].legend(handles=legend_handles, **legend_opt)

    side = 0.01
    if colorbar:
        f.suptitle(title, fontsize=9, y=0.975)
        plt.subplots_adjust(
            wspace=0.025, left=side, right=1 - side, bottom=0.31, top=0.81
        )

    else:
        f.suptitle(title, fontsize=9, y=0.975)
        plt.subplots_adjust(
            wspace=0.025, left=side, right=1 - side, bottom=0.09, top=0.77
        )

    mpu.set_map_layout(axes, width=18)

    f.canvas.draw()

    if colorbar:
        return cbar


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
    hatch_simple=None,
    add_legend=False,
    plotfunc="pcolormesh",
    colorbar_kwargs=None,
    legend_kwargs=None,   
    **kwargs,
):

    if average != "mean":
        title += f" – {average}"

    f, axes = plt.subplots(1, 3, subplot_kw=dict(projection=ccrs.Robinson()))

    axes = axes.flatten()
    
    if colorbar_kwargs is None:
        colorbar_kwargs = dict()
    
    if legend_kwargs is None:
        legend_kwargs = dict()

    for i in range(3):

        h, legend_handle = one_map(
            da=at_warming_c[i],
            ax=axes[i],
            average=average,
            levels=levels,
            mask_ocean=mask_ocean,
            ocean_kws=ocean_kws,
            skipna=skipna,
            hatch_simple=hatch_simple,
            plotfunc=plotfunc,
            **kwargs,
        )

    for ax in axes:
        # ax.coastlines(zorder=4, lw=0.5)
        ax.set_global()

    if colorbar:
        factor = 0.66 if add_legend else 1
        ax2 = axes[1] if add_legend else axes[2]

        colorbar_opt = dict(
            mappable=h,
            ax1=axes[0],
            ax2=ax2,
            size=0.15,
            shrink=0.25 * factor,
            orientation="horizontal",
            pad=0.1,
        )
        colorbar_opt.update(colorbar_kwargs)
        cbar = mpu.colorbar(**colorbar_opt)

        cbar.set_label(unit, labelpad=1, size=9)
        cbar.ax.tick_params(labelsize=9)  # , length=0)

    if add_legend and (not colorbar or hatch_simple is None):
        raise ValueError("Can only add legend when colorbar and add_hatch is True")

    if add_legend:

        h0 = text_legend(ax, "Color", "High model agreement", size=7)

        legend_opt = dict(
            handlelength=2.4,
            handleheight=1.3,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.45),
            fontsize=8.5,
            borderaxespad=0,
            frameon=True,
            handler_map={mpl.text.Text: TextHandler()},
            ncol=1,        
        )
        
        legend_opt.update(legend_kwargs)
        
        axes[2].legend(handles=[h0, legend_handle], **legend_opt)

    axes[0].set_title("At 1.5°C global warming", fontsize=9, pad=4)
    axes[1].set_title("At 2.0°C global warming", fontsize=9, pad=4)
    axes[2].set_title("At 4.0°C global warming", fontsize=9, pad=4)

    axes[0].set_title("(a)", fontsize=9, pad=4, loc="left")
    axes[1].set_title("(b)", fontsize=9, pad=4, loc="left")
    axes[2].set_title("(c)", fontsize=9, pad=4, loc="left")

    # axes[0].set_title("Tglob anomaly +1.5 °C", fontsize=9, pad=2)
    # axes[1].set_title("Tglob anomaly +2.0 °C", fontsize=9, pad=2)
    # axes[2].set_title("Tglob anomaly +4.0 °C", fontsize=9, pad=2)

    side = 0.01
    if colorbar:
        f.suptitle(title, fontsize=9, y=0.975)
        plt.subplots_adjust(
            wspace=0.025, left=side, right=1 - side, bottom=0.3, top=0.82
        )

    else:
        f.suptitle(title, fontsize=9, y=0.975)
        plt.subplots_adjust(
            wspace=0.025, left=side, right=1 - side, bottom=0.08, top=0.77
        )

    mpu.set_map_layout(axes, width=18)

    f.canvas.draw()

    if colorbar:
        return cbar


# ======================================================================================
# ======================================================================================
# ======================================================================================
# ======================================================================================
# ======================================================================================

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

    mpu.set_map_layout(axes, width=18)

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

    mpu.set_map_layout(axes, width=18)

    f.canvas.draw()

    return cbar

# options and function for Africa fact sheet plot (for Izidine)

AFRICA_KWARGS=dict(
    add_n_models=False,
    colorbar_kwargs=dict(size=0.075, pad=0.025),
    legend_kwargs=dict(
        bbox_to_anchor=(0.5, -0.225),
    ),
)


def set_extent_africa():
    # set extent to african domain
    # for Izidine

    f = plt.gcf()
    axes = np.array(f.axes)[:3]

    for ax in axes:
        ax.set_extent([-25, 65, -36, 38], ccrs.PlateCarree())

    side = 0.02
    plt.subplots_adjust(wspace=0.025, left=side, right=1 - side, bottom=0.2, top=0.85)
    mpu.set_map_layout(axes, width=17)

    f.canvas.draw()


