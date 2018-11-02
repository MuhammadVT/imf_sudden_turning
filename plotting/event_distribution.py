import matplotlib
matplotlib.use("Agg")
import sys
sys.path.append("../data/")
from build_event_database import build_event_database
import pandas as pd
import numpy as np
import datetime as dt
from matplotlib.ticker import MultipleLocator
import matplotlib.pyplot as plt
plt.style.use("ggplot")

IMF_turning = "northward"
#IMF_turning = "southward"
event_status = "good"
#rads = "all"
rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw"]
geomag_condition="quiet"

stable_interval=30
df = build_event_database(IMF_turning=IMF_turning, event_status=event_status,
                          geomag_condition=geomag_condition, rads=rads)
nevents = len(df.datetime.unique())
dtms = [pd.to_datetime(x) for x in df.datetime.unique()]
yrs = [x.year for x in dtms]
mns = [x.month for x in dtms]
hrs = [(x.hour + x.minute/60.) for x in dtms]
rad_to_num = {rads[i]:i+1 for i in range(len(rads))}
rads_num = [rad_to_num[x] for x in df.rad.values]

fig_dir = "../plots/event_distribution/"

# Plot event dist. by UT
fig, ax = plt.subplots()
bins = np.arange(3, 14, 0.5)
ax.hist(hrs, bins=bins, rwidth=0.97)
ax.set_ylim([0, 4.2])
ax.set_xlabel("UT Time [Hour]")
ax.set_ylabel("Count")
ax.set_title(str(nevents) + " " + IMF_turning.capitalize() + " IMF Turnings")
ax.xaxis.set_major_locator(MultipleLocator(1))
ax.yaxis.set_major_locator(MultipleLocator(1))
fig_name = "ut_dist_" + str(nevents) + "_" + IMF_turning + "_turnings_IMF"
fig.savefig(fig_dir + fig_name + ".png", dpi=200, bbox_inches="tight")
plt.close(fig)


# Plot event dist. by month
fig, ax = plt.subplots()
bins = np.arange(0.5, 13, 1)
ax.hist(mns, bins=bins, rwidth=0.97)
ax.set_ylim([0, 10.2])
ax.set_xlabel("Month")
ax.set_ylabel("Count")
ax.set_title(str(nevents) + " " + IMF_turning.capitalize() + " IMF Turnings")
ax.xaxis.set_major_locator(MultipleLocator(1))
ax.yaxis.set_major_locator(MultipleLocator(1))
fig_name = "month_dist_" + str(nevents) + "_" + IMF_turning + "_turnings_IMF"
fig.savefig(fig_dir + fig_name + ".png", dpi=200, bbox_inches="tight")
plt.close(fig)


# Plot event dist. by year 
fig, ax = plt.subplots()
bins = np.arange(2009.5, 2018.5, 1)
ax.hist(yrs, bins=bins, rwidth=0.98)
ax.set_ylim([0, 12.2])
ax.set_xlabel("Year")
ax.set_ylabel("Count")
ax.set_title(str(nevents) + " " + IMF_turning.capitalize() + " IMF Turnings")
ax.xaxis.set_major_locator(MultipleLocator(1))
ax.yaxis.set_major_locator(MultipleLocator(1))
fig_name = "year_dist_" + str(nevents) + "_" + IMF_turning + "_turnings_IMF"
fig.savefig(fig_dir + fig_name + ".png", dpi=200, bbox_inches="tight")
plt.close(fig)

# Plot event dist. by radars
fig, ax = plt.subplots()
bins = np.arange(0.5, 7, 1)
ax.hist(rads_num, bins=bins, rwidth=0.98)
ax.set_ylim([0, 30])
ax.set_xlabel("Radar")
ax.set_ylabel("Count")
ax.set_title(str(nevents) + " " + IMF_turning.capitalize() + " IMF Turnings")
ax.yaxis.set_major_locator(MultipleLocator(4))
ax.set_xticklabels([""] + rads)
fig_name = "radar_dist_" + str(nevents) + "_" + IMF_turning + "_turnings_IMF"
fig.savefig(fig_dir + fig_name + ".png", dpi=200, bbox_inches="tight")
plt.close(fig)

