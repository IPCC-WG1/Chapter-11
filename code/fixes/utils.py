import glob
import parse
import matplotlib.pyplot as plt
import xarray as xr


def _glob(folder):
    return sorted(glob.glob(folder))


def list_files_monotonic(folder):

    files = _glob(folder)

    for fN in files:
        ds = xr.open_dataset(fN, decode_cf=False)

        cal = ds["time"].calendar
        len_time = len(ds["time"])

        ds = xr.conventions.decode_cf(ds, use_cftime=True)
        index = ds.indexes["time"]
        print(
            fN,
            index.is_monotonic,
            index.is_monotonic_decreasing,
            index.is_monotonic_increasing,
            cal,
            len_time,
        )


def plot_time_spans_folder(folder):

    files = _glob(folder)

    parsed_time = parse_filename_time(files)

    plot_filename_time(parsed_time)


def parse_filename_time(files):

    fmt = "{}_{:4d}{:2d}{:2d}-{:4d}{:2d}{:2d}.nc"

    return [parse.parse(fmt, f).fixed[1:] for f in files]


def plot_filename_time(result):

    ax = plt.gca()

    for i, r in enumerate(result):
        beg = (r[0] * 365 + (r[1] - 1) * 30 + r[2]) / 365
        end = (r[3] * 365 + (r[4] - 1) * 30 + r[5]) / 365
        ax.plot([beg, end], [i, i], lw=10, solid_capstyle="butt")
