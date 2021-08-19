import matplotlib as mpl
import matplotlib.pyplot as plt
import mplotutils as mpu
import numpy as np
import regionmask
import seaborn as sns
import xarray as xr

from . import plot

REGION_GROUPS = dict(
    NAmerica=[1, 2, 3, 4, 5],
    CAmerica=[6, 7, 8],
    NSAmerica=[9, 10, 11, 12],
    SSAmerica=[13, 14, 15],
    EU=[16, 17, 18, 19],
    NAfrica=[20, 21, 22, 23, 24],
    SAfrica=[25, 26, 27],
    NAsia=[28, 29, 30, 31],
    CAsia=[32, 33, 34, 36],
    EAsia=[35, 37, 38],
    AUS=[39, 40, 41, 42, 43],
)

REGION_NAMES = dict(
    NAmerica="North America",
    CAmerica="Central America",
    NSAmerica="Northern South America",
    SSAmerica="Southern South America",
    EU="Europe",
    NAfrica="Northern Africa",
    SAfrica="Southern Africa",
    NAsia="Northern Asia",
    CAsia="Middle East/ Central Asia",
    EAsia="India/ Southeast Asia",
    AUS="Australasia",
)


def _prep_for_concat(da):
    to_drop = ["ensnumber", "table", "grid", "varn", "ensi", "postprocess"]

    if "type" in da.coords.keys():
        to_drop.append("type")

    da = da.set_index(mod_ens=("model", "exp", "ens"))
    return da.drop_vars(to_drop)


def concat_for_scaling(c6_at_warming_, warming_levels):

    c6_at_warming_ = [_prep_for_concat(ds) for ds in c6_at_warming_]

    c6_at_warming_ = xr.concat(c6_at_warming_, dim="wl", join="outer")
    c6_at_warming_ = c6_at_warming_.reset_index("mod_ens")
    return c6_at_warming_.assign_coords(wl=warming_levels)


def plot_reg_full(ax, at_warming, region, conf_cmip):

    lw = 1.5

    reg = at_warming.sel(region=region)
    opt_ens = dict(color="0.5", lw=0.5)

    for i in range(len(reg.mod_ens)):

        reg.sel(mod_ens=i).plot(ax=ax, **opt_ens)

    h0 = ax.plot([], [], **opt_ens, label="Ensemble member")

    h1 = reg.median("mod_ens").plot(
        ax=ax, color="k", label="Multi-model-median", lw=1.5
    )

    d = reg.sel(mod_ens=reg.exp == "ssp126").median("mod_ens")
    h2 = d.plot(
        ax=ax, color=conf_cmip.colors["ssp126"], label="SSP1-2.6", zorder=4, lw=lw
    )

    d = reg.sel(mod_ens=reg.exp == "ssp245").median("mod_ens")
    h3 = d.plot(
        ax=ax, color=conf_cmip.colors["ssp245"], label="SSP2-4.5", zorder=3, lw=lw
    )

    d = reg.sel(mod_ens=reg.exp == "ssp585").median("mod_ens")
    h4 = d.plot(
        ax=ax, color=conf_cmip.colors["ssp585"], label="SSP5-8.5", zorder=2, lw=lw
    )

    ax.set_title(reg.names.item(), fontsize=9)

    return [h0[0], h1[0]], [h2[0], h3[0], h4[0]]


def plot_region_groups():

    colors = sns.color_palette("Paired", 12)

    colors = colors[:10] + colors[-1:]

    f, ax = plot.map_subplots()

    ar6_land = regionmask.defined_regions.ar6.land

    for i, (key, region) in enumerate(REGION_GROUPS.items()):
        text_kws = dict(color=colors[i], fontsize=8, bbox=dict(pad=0.2, color="w"))
        ar6_land[region].plot(
            ax=ax,
            text_kws=text_kws,
            line_kws=dict(linewidth=1, color=colors[i], label=REGION_NAMES[key]),
            label="abbrev",
        )

    ax.set_global()

    f.subplots_adjust(left=0.01, right=0.99)

    ax.legend(fontsize=7, loc="lower left", ncol=1)

    mpu.set_map_layout(np.array([ax]))

    ax.set_title("")


def plot_scaling(c6_at_warming_, conf_cmip, title, ylabel, legend_opt=None):

    # ====
    # options

    colors = [
        np.array([221, 84, 46]) / 255,
        np.array([33, 52, 219]) / 255,
        np.array([53, 165, 197]) / 255,
        np.array([170, 24, 24]) / 255,
        np.array([8, 46, 114]) / 255,
    ]

    if legend_opt is None:
        legend_opt = dict(
            borderaxespad=0.2,
            fontsize=8,
            #     borderpad=0,
            handlelength=1.0,  # mpl.rcParams["legend.handlelength"],
            labelspacing=0.75 * mpl.rcParams["legend.labelspacing"],
            handletextpad=0.6 * mpl.rcParams["legend.handletextpad"],
        )

    # ====

    c6_at_warming_mmm_ = c6_at_warming_.median("mod_ens")

    # ====

    f, axes_ = plt.subplots(4, 3, constrained_layout=True, sharex=True, sharey=True)

    f.set_size_inches(18 / 2.54, 20 / 2.54)

    axes = axes_.flatten()

    ax = axes[0]
    l1, l2 = plot_reg_full(ax, c6_at_warming_, region=-1, conf_cmip=conf_cmip)

    legend_handle0 = ax.legend(handles=l1, loc="upper left", **legend_opt)
    ax.add_artist(legend_handle0)

    ax.legend(handles=l2, loc="lower right", **legend_opt)

    ax.set_xlabel("")
    ax.set_ylabel("")

    for i, region_name in enumerate(REGION_GROUPS.keys(), 1):

        ax = axes[i]

        for j, r in enumerate(REGION_GROUPS[region_name]):
            d = c6_at_warming_mmm_.sel(region=r)

            slope = d.polyfit("wl", 1).polyfit_coefficients.sel(degree=1).item()
            label = "{} ({:0.1f}°C/°C)".format(d.abbrevs.item(), slope)

            d.plot(ax=ax, label=label, lw=1, color=colors[j])
        leg = ax.legend(loc="upper left", **legend_opt)
        leg.set_zorder(0)
        ax.set_title(REGION_NAMES[region_name], fontsize=9)
        ax.set_xlabel("")
        ax.set_ylabel("")

    f.suptitle(title, fontsize=11)

    for ax in axes_[:, 0]:
        ax.set_ylabel(ylabel, fontsize=9)
        ax.tick_params(labelsize=9)  # , length=0)

    for ax in axes_[-1, :]:
        ax.set_xlabel("Global warming level (°C)", fontsize=9)
        ax.tick_params(labelsize=9)  # , length=0)

    letters = "abcdefghijklmnop"
    for i, ax in enumerate(axes):
        ax.set_title(f"({letters[i]})", loc="left", fontsize=9)
        ax.axline((0, 0), slope=1, color="0.1", lw=1.5, label="1-to-1 line")
    sns.despine(f)

    ax = axes[0]

    for s in ax.spines.values():
        s.set_lw(1.25)
