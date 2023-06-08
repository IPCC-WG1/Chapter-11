import copy

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

# asymetric colors are desired

# COLORS LEFT
# ===========

delta = 0.15
n = 50

BrBG = copy.copy(plt.cm.BrBG)

colors = BrBG(np.linspace(0 + delta, 1 - delta, n))
colors_left = colors[:25, :]

# COLORS RIGHT
# ============

delta = 0.255
n = 50

BrBG = copy.copy(plt.cm.BrBG)

colors = BrBG(np.linspace(0 + delta, 1 - delta, n))
colors_right = colors[25:, :]

colors = np.concatenate([colors_left, colors_right])

cmap = mpl.colors.LinearSegmentedColormap.from_list("BrBG_cut", colors)
