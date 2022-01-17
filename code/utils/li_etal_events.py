import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# used for Figure_11.12 andFigure_11.15

fN = (
    "../data/Li_etal/"
    "frequency_intensity_changes_TXx_Rx1day_events_1851_vs_1850_orig_from_chao.xlsx"
)

MAPPING = {
    "f_10": "frequency 10 year event",
    "f_50": "frequency 50 year event",
    "i_10": "intensity 10 year event",
    "i_50": "intensity 50 year event",
}


# first row of the data - depending on category
ROWS = {"f_10": 1, "f_50": 8, "i_10": 16, "i_50": 23}

# define colors
MAGENTA = dict(fc="#cab2d6", ec="#6a3d9a")
ORANGE = dict(fc="#fdbf6f", ec="#ff7f00")


def read_data(varn):
    """read data from excel file in ../data/Li_etal/"""

    data = dict()
    for key, row in ROWS.items():

        dta = pd.read_excel(
            fN, sheet_name=varn, usecols="H:M", header=row + 1, nrows=5, index_col=0
        )

        dta.columns = [0.5, 0.17, 0.83, 0.05, 0.95]
        dta.index.name = "warming_level"
        data[key] = dta

    return data


# read data from dataframe
def get_boxtats(data):
    """prepare data for ax.bxp plot"""

    out = list()
    for key, row in data.iterrows():
        bxpstats = dict(
            med=row[0.5],
            q1=row[0.17],
            q3=row[0.83],
            whislo=row[0.05],
            whishi=row[0.95],
            fliers=[],
            label=key,
        )
        out.append(bxpstats)

    return out


def plot_boxstats(data, title, unit):
    """create boxplot for data from Li et al

    Parameters
    ----------
    data : pd.DataFrame
        From read_data
    title : str
        axes title
    unit : str
        unit for y_label
    """
    def box_opt(fc, ec):

        return dict(
            showcaps=False,
            boxprops=dict(fc=fc, ec=ec),
            medianprops=dict(color=ec),
            whiskerprops=dict(color=ec),
            patch_artist=True,
            widths=0.6,
        )

    # ===

    f, axes = plt.subplots(1, 1, constrained_layout=True)

    f.set_size_inches(9 / 2.54, 7.25 / 2.54)

    ax = axes

    x = np.arange(5) * 2

    dx = 0.4

    bp1 = ax.bxp(get_boxtats(data["i_10"]), positions=x - dx, **box_opt(**MAGENTA))
    bp2 = ax.bxp(get_boxtats(data["i_50"]), positions=x + dx, **box_opt(**ORANGE))

    ax.set_xticks(x)

    ax.legend(
        [bp1["boxes"][0], bp2["boxes"][0]],
        ["10-year event", "50-year event"],
        loc="upper left",
    )

    ax.set_xlabel("Global warming level above 1850–1900 (°C)", size=9)
    ax.set_ylabel(f"Increase in event intensity ({unit})", size=9)
    ax.tick_params(axis="both", which="major", labelsize=9)

    ax.set_title(title, size=9)

    sns.despine(f)
