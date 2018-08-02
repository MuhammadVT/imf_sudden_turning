import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from matplotlib.ticker import MultipleLocator
from velcomp_vs_reltime import fetch_data
plt.style.use("ggplot")

def vel_vs_mlt(ax, data_dict, veldir="zonal", sampling_method="median",
               glatc_list=None, title="xxx", add_err_bar=False, lat_avg=False,
               color_list=None, marker_size=2, marker_type="o"):

    """ plots flow components vs relative times
    for a given MLT range at a given MLAT.

    parameters
    ----------
    veldir : str
        veocity component. if set to "all" then it means the velocity magnitude
    """

    from matplotlib.collections import PolyCollection,LineCollection

    # calculate velocity components
    vel_mag = data_dict['vel_mag']
    vel_dir = np.deg2rad(data_dict['vel_dir'])
    vel_mag_err = data_dict['vel_mag_err']

    if veldir == "zonal":
        vel_comp = vel_mag*(-1.0)*np.sin(vel_dir)
        vel_comp_err = vel_mag_err*(-1.0)*np.sin(vel_dir)
    elif veldir == "meridional":
        vel_comp = vel_mag*(-1.0)*np.cos(vel_dir)
        vel_comp_err = vel_mag_err*(-1.0)*np.cos(vel_dir)
    elif veldir == "all":
        vel_comp = np.abs(vel_mag)
        vel_comp_err = vel_mag_err
    vel_mlt = data_dict['glonc'] / 15.

    # colors of different lines
    if color_list is None:
        color_list = ['darkblue', 'b', 'dodgerblue', 'c', 'g', 'orange', 'r']
    color_list.reverse()

    # MLATs
    if glatc_list is None:
        glatc_list = np.array([55.5])
    if lat_avg:
        # center at 0 MLT
        vel_mlt_tmp = [x if x <=12 else x-24 for x in vel_mlt]

        xs = []
        ys = []
        for mlt_tmp in np.unique(vel_mlt_tmp):
            xs.append(mlt_tmp)
            if sampling_method == "median":
                # Do median filter
                ys.append(np.median(vel_comp[np.where(vel_mlt_tmp == mlt_tmp)]))
            if sampling_method == "mean":
                ys.append(np.mean(vel_comp[np.where(vel_mlt_tmp == mlt_tmp)]))

        # plot the velocities for each MLAT
        ax.plot(xs, ys, color="k",
                marker=marker_type, ms=marker_size, linewidth=2.0)

    else:
        for jj, mlat in enumerate(glatc_list):
            vel_comp_jj = np.array([vel_comp[i] for i in range(len(vel_comp)) if data_dict['glatc'][i] == mlat])
            vel_mlt_jj = np.array([vel_mlt[i] for i in range(len(vel_comp)) if data_dict['glatc'][i] == mlat])
            # center at 0 MLT
            vel_mlt_jj = [x if x <=12 else x-24 for x in vel_mlt_jj]
            vel_comp_err_jj = np.array([vel_comp_err[i] for i in range(len(vel_comp_err)) if data_dict['glatc'][i] == mlat])

            xs = []
            ys = []
            for mlt_tmp in np.unique(vel_mlt_jj):
                xs.append(mlt_tmp)
                if sampling_method == "median":
                    # Do median filter
                    ys.append(np.median(vel_comp_jj[np.where(vel_mlt_jj == mlt_tmp)]))
                if sampling_method == "mean":
                    ys.append(np.mean(vel_comp_jj[np.where(vel_mlt_jj == mlt_tmp)]))

            # plot the velocities for each MLAT
            ax.plot(xs, ys, color=color_list[jj],
                    marker=marker_type, ms=marker_size, linewidth=2.0, label=str(mlat))

            if add_err_bar:
                ax.errorbar(vel_mlt_jj, vel_comp_jj, yerr=vel_comp_err_jj, mfc=color_list[jj],
                        #marker='o', s=3, linewidths=.5, edgecolors='face', label=str(int(mlat)))
                        fmt=marker_type, ms=marker_size, elinewidth=.5, mec=color_list[jj], ecolor="k")

    # add text
    ax.set_title(title, fontsize=14)

    # Set xtick directions
    ax.tick_params(direction="in")

    # Set ytick format
    ax.yaxis.set_major_locator(MultipleLocator(20))

    # add zero-line
    if veldir != "all":
        ax.axhline(y=0, color='k', linewidth=0.7)

    # set axis limits
    ax.set_xlim([-6, 6])

    # add legend
    #ax.legend(loc="upper right", fontsize=8)
    #ax.legend(loc="best", fontsize=8)

    # axis labels
    ax.set_ylabel("Vel [m/s]")

    return

if __name__ == "__main__":

    # input parameters
    nvel_min=20
    lat_range=[54, 59]
    #lat_range=[54, 58]
    lat_avg = False
    mlt_range = [-8, 8]
    lat_min = 50
    reltime_resolution=2
    mlt_width=2.    # MLT width used within which fitting a cos-curve takes place
    gazmc_span_minlim=30
    vel_mag_err_maxlim=0.5
    vel_mag_maxlim=200.
    fit_by_bmazm=False
    fit_by_losvel_azm=True
    add_err_bar = False
    ftype = "fitacf"
    coords = "mlt"
    #weighting = None 
    weighting = "std"
    sampling_method = "median"
    #sampling_method = "mean"
    dbdir="../data/sqlite3/"
    IMF_turnings = ["northward", "southward"]
    veldirs = ["zonal", "meridional"]

    reltime_list = [-10, 10]
    #reltime_list = [-16, 16]
    #reltime_list = [-20, 20]
    #reltime_list = [-26, 26]
    del_reltime = 4
    reltime_ranges = []
    for rltm in reltime_list:
        reltime_ranges.append([-del_reltime + rltm, del_reltime + rltm])

    glatc_list = np.arange(lat_range[1], lat_range[0], -1) - 0.5 

    for IMF_turning in IMF_turnings:
        input_table = "master_cosfit_superposed_" + IMF_turning + "_mltwidth_" + str(int(mlt_width)) +\
                       "_res_" + str(reltime_resolution) + "min"

        for veldir in veldirs:
            if veldir == "zonal":
                ylim = [-180, 80]
            else:
                ylim = [-80, 80]

            fig, axes = plt.subplots(nrows=len(reltime_ranges), ncols=1, sharex=True, figsize=(6, 8))
            fig.subplots_adjust(hspace=0.2)
            for i, reltime_range in enumerate(reltime_ranges): 
                ax = axes[i]
                center_reltime = reltime_list[i]
                # fetches the data from db 
                data_dict = fetch_data(input_table, lat_range=lat_range, nvel_min=nvel_min,
                                       reltime_range=reltime_range, reltime_resolution=reltime_resolution,
                                       mlt_width=mlt_width, mlt_range=mlt_range, gazmc_span_minlim=gazmc_span_minlim,
                                       fit_by_bmazm=fit_by_bmazm, fit_by_losvel_azm=fit_by_losvel_azm,
                                       vel_mag_maxlim=vel_mag_maxlim,
                                       vel_mag_err_maxlim=vel_mag_err_maxlim,
                                       ftype=ftype, coords=coords, dbdir=dbdir,
                                       db_name=None, weighting=weighting)
                # plot the flow vector components
                if veldir == "all" :
                    title = "Velocity Magnitude, Time = " + str(center_reltime)
                else:
                    title = veldir[0].upper()+veldir[1:] + " Velocities, Time = " + str(center_reltime)
                vel_vs_mlt(ax, data_dict, veldir=veldir, sampling_method=sampling_method, lat_avg=lat_avg,
                           glatc_list=glatc_list, title=title, add_err_bar=add_err_bar)
                ax.yaxis.set_major_locator(MultipleLocator(base=40))

                if veldir == "all":
                    ax.set_ylim([0, 200])
                else:
                    ax.set_ylim(ylim)

                ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=12)
                #ax.legend(loc="best", fontsize=12)

            # set axis label
            axes[-1].set_xlabel("MLT")
            axes[-1].xaxis.set_major_locator(MultipleLocator(base=3))
            xlabels = [item.get_text() for item in axes[-1].get_xticklabels()]
            xlabels = [str(x) for x in range(18, 24, 3) + range(0, 9, 3)]
            plt.xticks(range(-6, 9, 3), xlabels)

            # save the fig
            fig_dir = "/home/muhammad/Dropbox/tmp/velcomp_vs_mlt/"
            #fig_name = "line_plot"
            if lat_avg:
                tmp_txt = "_lat_avg"
            else:
                tmp_txt = ""
            fig_name = "velcomp_vs_mlt_" + veldir + "_" + IMF_turning + tmp_txt + "_lat" + str(lat_range[0]) +\
                       "_to_lat" + str(lat_range[1]) +\
                       "_mltwidth_" + str(int(mlt_width)) +\
                       "_res_" + str(reltime_resolution) + "min" + \
                       "_nvel_min_" + str(nvel_min) + "velmag_err_"  + str(vel_mag_err_maxlim) +\
                       "_center_reltm_" + str(center_reltime) + "_" + sampling_method

            fig.savefig(fig_dir + fig_name + ".png", dpi=300, bbox_inches="tight")
            plt.close(fig)
            #plt.show()


