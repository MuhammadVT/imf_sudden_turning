import sys
sys.path.append("../data/")
from build_event_database import build_event_database
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import matplotlib.pyplot as plt
plt.style.use("ggplot")
#plt.style.use("fivethirtyeight")

from superpose_symh import plot_superposed_symh
from superpose_imf import plot_superposed_imf
from superpose_aualae_symh import plot_superposed_aualae


"""NOTE: Run on sd-work4"""

params = ["Bz", "au", "al", "ae", "symh"]
ylim_imf=[-12, 12]
ylim_au=[-50, 500]; ylim_al=[-500, 50]
ylim_ae = [-50, 800]; ylim_symh = [-80, 30]
ylims = [ylim_imf, ylim_au, ylim_al, ylim_ae, ylim_symh]

IMF_turning = "southward"
#IMF_turning = "northward"
event_status = "good"
#rads = "all"
rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw"]
geomag_condition="quiet"

stable_interval=30
df_events = build_event_database(IMF_turning=IMF_turning, event_status=event_status,
                                 geomag_condition=geomag_condition, rads=rads)
nevents = len(df_events.datetime.unique())
fig, axes = plt.subplots(nrows=len(params), ncols=1, sharex=True, figsize=(5,10))
fig.subplots_adjust(hspace=0.1)
if len(params) == 1:
    axes = [axes]
for i, param in enumerate(params):
    ax = axes[i]
    if param == "Bz":
        plot_superposed_imf(ax, imf_comp=param, df_turn=df_events,
                            stable_interval=stable_interval, IMF_turning=IMF_turning, color="r")
    elif param == "symh":
        plot_superposed_symh(ax, df_turn=df_events,
                             stable_interval=stable_interval, IMF_turning=IMF_turning, color="orange")

    else:
        plot_superposed_aualae(ax, param=param, df_turn=df_events,
                            stable_interval=stable_interval, IMF_turning=IMF_turning)
    ax.set_ylim(ylims[i])
    if param == "Bz":
        ax.set_ylabel("Bz [nT]")
    else:
        ax.set_ylabel(param.upper() + " [nT]")
axes[0].set_title(str(nevents) + " " + IMF_turning.capitalize() + " IMF Turnings")
axes[-1].set_xlabel("Relative Time [min]")

#fig_dir = "/home/muhammad/Dropbox/tmp/tmp/"
fig_dir = "../plots/superposed_imf_aualae_symh/"
fig_name = "imf_aualae_symh_for_" + str(nevents) + "_" + IMF_turning + "_imf_turnings" 
fig.savefig(fig_dir + fig_name + ".png", bbox_inches="tight")
plt.close(fig)
#plt.show()



