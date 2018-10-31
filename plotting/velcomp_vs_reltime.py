import matplotlib
matplotlib.use("Agg")
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
from matplotlib.ticker import MultipleLocator
plt.style.use("ggplot")


def fetch_data(input_table, lat_range=[53, 59], nvel_min=3,
               reltime_range=[-30, 30],
               reltime_resolution=2, mlt_width=1.,
               mlt_range=[-3.2, -2.8], gazmc_span_minlim=10, 
               vel_mag_maxlim=500., vel_mag_err_maxlim=0.2,
               fit_by_bmazm=False, fit_by_losvel_azm=True,
               ftype="fitacf", coords="mlt",
               dbdir="../data/sqlite3/", db_name=None, weighting=None):

    """ Fetches fitted data at a given MLAT ponit and MLT range 
        from the master db into a dict.

    Parameters
    ----------
    input_table : str
        A table name in db_name db
    lat_ragne : list
        The range of latitudes of interest
    nvel_min : int
        minimum requirement for the number of velocity measurements
        in a lat-lon grid cell
    reltime_range : list
        The range of relative time of interest
    reltime_resolution : int
        The time resolution (in minutes) of the fitted data
    mlt_range : list
        The MLT range of interest
    mlt_width : float
        The width of MLT region within which cosine fitting will be performed.
        i.e., for a point at 1 MLT, the points within 1  +/- 0.5*mlt_width will be
        used to fit a cosine curve.
    fit_by_bmazm : bool (Default to False)
        If set to true, the cosfitting would be done on losvel-vs-magbmazm.
    fit_by_losvel_azm : bool (Default to True)
        If set to true, the cosfitting would be done on losvel-vs-losvelazm.
    db_name : str, default to None
        Name of the master db
    ftype : str
        SuperDARN file type
    coords : str
        Coordinates in which the binning process took place.
        Default to "mlt, can be "geo" as well.
    weighting : str (Default to None)
        Type of weighting used for curve fitting
        if set to None, all azimuthal bins are
        considered equal regardless of the nubmer of points
        each of them contains.


    Return
    ------
    data_dict : dict

    """

    # construct a db name
    if db_name is None:
        db_name = "sd_master_" + coords + "_" +ftype + ".sqlite"

    # make a db connection
    try:
        conn = sqlite3.connect(dbdir + db_name,
                               detect_types = sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    # set input_table name
    if weighting is not None:
        input_table = input_table + "_" + weighting + "_weight"

    # add new columns
    if coords == "mlt":
        col_glatc = "mag_glatc"   # glatc -> gridded latitude center
        col_gltc = "mag_gltc"     # mlt hour in degrees
        col_gazmc = "mag_gazmc"   # gazmc -> gridded azimuthal center
        col_gazmc_count = "mag_gazmc_count"
        col_azmc_span = "mag_gazmc_span"
    if coords == "geo":
        col_glatc = "geo_glatc"
        col_gltc = "geo_gltc"    # local time in degrees
        col_gazmc = "geo_gazmc"
        col_azmc_span = "geo_gazmc_span"

    # select data from the master_cosfit table for the MLT region of interest
    gltc_left = (mlt_range[0] * 15.) % 360
    gltc_right = (mlt_range[1] * 15.) % 360
    if gltc_right < gltc_left:
        where_clause = "AND (({gltc} BETWEEN {gltc_left} AND 360) or ({gltc} BETWEEN 0 AND {gltc_right})) "
    else:
        where_clause = "AND ({gltc} BETWEEN {gltc_left} AND {gltc_right}) "
    command = "SELECT vel_count, vel_mag, vel_dir, {glatc}, {gltc}, " +\
              "vel_mag_err, vel_dir_err, relative_time, {azmc_span_txt}, ABS(vel_mag_err/vel_mag) FROM {tb1} " +\
              "WHERE ({glatc} BETWEEN {lat_min} AND {lat_max}) " +\
              where_clause +\
              "AND {azmc_span_txt} >= {gazmc_span_minlim} "+\
              "AND vel_count >= {nvel_min} "+\
              "AND ABS(vel_mag_err/vel_mag) <= {vel_mag_err_maxlim} "+\
              "AND ABS(vel_mag) <= {vel_mag_maxlim} "+\
              "AND relative_time BETWEEN {reltime_left} AND {reltime_right}"
    command = command.format(tb1=input_table, glatc=col_glatc,
                             gltc = col_gltc, lat_min=lat_range[0],
                             lat_max=lat_range[1], nvel_min=nvel_min,
                             vel_mag_err_maxlim=vel_mag_err_maxlim,
                             vel_mag_maxlim=vel_mag_maxlim,
                             gltc_left=gltc_left, gltc_right=gltc_right,
                             azmc_span_txt=col_azmc_span, gazmc_span_minlim=gazmc_span_minlim,
                             reltime_left=reltime_range[0], reltime_right=reltime_range[1])
    cur.execute(command)
    rws = cur.fetchall()

    data_dict = {}
    # filter the data based on lattitude range. 
    data_dict['vel_count'] = np.array([x[0] for x in rws])
    data_dict['vel_mag'] = np.array([x[1] for x in rws])
    data_dict['vel_dir'] = np.array([x[2] for x in rws])
    data_dict['glatc'] = np.array([x[3] for x in rws])
    data_dict['glonc'] = np.array([x[4] for x in rws])
    data_dict['vel_mag_err'] = np.array([x[5] for x in rws])
    data_dict['vel_dir_err'] = np.array([x[6] for x in rws])
    data_dict['relative_time'] = np.array([x[7] for x in rws])
    data_dict['gazmc_span'] = np.array([x[8] for x in rws])
    data_dict['vel_mag_err_ratio'] = np.array([x[9] for x in rws])

    # close db connection
    conn.close()

    return data_dict

def vel_vs_reltime(ax, data_dict, mlt_range=[-3.2, -2.8], IMF_turning="southward",
                   veldir="zonal", sampling_method="median",
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
    vel_reltime = data_dict['relative_time']


    # colors of different lines
    if color_list is None:
        color_list = ['darkblue', 'b', 'dodgerblue', 'c', 'g', 'orange', 'r']
    color_list.reverse()

    # MLATs
    if glatc_list is None:
        glatc_list = np.array([55.5])
    if lat_avg:
        xs = []
        ys = []
        for reltm in np.unique(vel_reltime):
            # NOTE: Do moving average to smoothen the curve
            if (reltm >= -2) and (reltm <= 5):
                #vel_tmp = vel_comp[np.where(vel_reltime == reltm)]
                vel_tmp = vel_comp[np.where((vel_reltime >= reltm-1) & (vel_reltime <= reltm+1))]
            else:
                vel_tmp = vel_comp[np.where((vel_reltime >= reltm-2) & (vel_reltime <= reltm+2))]

            xs.append(reltm)
            if sampling_method == "median":
                ys.append(np.median(vel_tmp))
            if sampling_method == "mean":
                ys.append(np.mean(vel_tmp))

        # plot the velocities for each MLAT
        ax.plot(xs, ys, color="k",
                marker=marker_type, ms=marker_size, linewidth=2.0)

        if add_err_bar:
            ax.errorbar(vel_reltime, vel_comp, yerr=vel_comp_err, mfc="k",
                    #marker='o', s=3, linewidths=.5, edgecolors='face', label=str(int(mlat)))
                    fmt=marker_type, ms=marker_size, elinewidth=.5, mec="k", ecolor="k")

    else:
        for jj, mlat in enumerate(glatc_list):
            vel_comp_jj = np.array([vel_comp[i] for i in range(len(vel_comp)) if data_dict['glatc'][i] == mlat])
            vel_reltime_jj = np.array([vel_reltime[i] for i in range(len(vel_comp)) if data_dict['glatc'][i] == mlat])
            vel_comp_err_jj = np.array([vel_comp_err[i] for i in range(len(vel_comp_err)) if data_dict['glatc'][i] == mlat])

            xs = []
            ys = []
            for reltm in np.unique(vel_reltime_jj):
                #NOTE: Do moving average to smoothen the curve
                if IMF_turning=="southward":
                    reltm_minlim = -2; reltm_maxlim = 4
                else:
                    reltm_minlim = -2; reltm_maxlim = 3
                if (reltm >= reltm_minlim) and (reltm <= reltm_maxlim):
                    #vel_tmp = vel_comp_jj[np.where(vel_reltime_jj == reltm)]
                    if IMF_turning=="southward":
                        vel_tmp = vel_comp_jj[np.where((vel_reltime_jj >= reltm-1) & (vel_reltime_jj <= reltm+1))]
                    else:
                        vel_tmp = vel_comp_jj[np.where((vel_reltime_jj >= reltm-1) & (vel_reltime_jj <= reltm+1))]
                else:
                    if mlat == 55.5: 
                        vel_tmp = vel_comp_jj[np.where((vel_reltime_jj >= reltm-4) & (vel_reltime_jj <= reltm+4))]
                    else:
                        vel_tmp = vel_comp_jj[np.where((vel_reltime_jj >= reltm-4) & (vel_reltime_jj <= reltm+4))]

                if sum(mlt_range) <= 2:
                    # NOTE: Remove errors by filtering out eastward flows before MLT <= 2 because the flow is westward
                    vel_tmp = vel_tmp[np.where(vel_tmp <= 30)]

                # NOTE: Remove errors by filtering out very large westward flows
                if IMF_turning=="southward":
                    vel_tmp = vel_tmp[np.where(vel_tmp >= -180)]
                else:
                    vel_tmp = vel_tmp[np.where(vel_tmp >= -150)]

                if sampling_method == "median":
                    # Do median filter
                    y_tmp = np.median(vel_tmp)
                    ys.append(y_tmp)
                    xs.append(reltm)
                if sampling_method == "mean":
                    y_tmp = np.mean(vel_tmp)
                    ys.append(y_tmp)
                    xs.append(reltm)
                if sampling_method is None:
                    y_tmp = vel_tmp
                    ys.extend(y_tmp)
                    xs.extend([reltm]*len(y_tmp))

            # plot the velocities for each MLAT
            # Do moving average before plotting
            ax.plot(xs, ys, color=color_list[jj],
                    marker=marker_type, ms=marker_size, linewidth=2.0, label=str(mlat))

            if add_err_bar:
                ax.errorbar(vel_reltime_jj, vel_comp_jj, yerr=vel_comp_err_jj, mfc=color_list[jj],
                        #marker='o', s=3, linewidths=.5, edgecolors='face', label=str(int(mlat)))
                        fmt=marker_type, ms=marker_size, elinewidth=.5, mec=color_list[jj], ecolor="k")


    # add text
    ax.set_title(title, fontsize="small")

    # Set xtick directions
    ax.tick_params(direction="in")

    # Set ytick format
    ax.yaxis.set_major_locator(MultipleLocator(20))

    # add zero-line
    if veldir != "all":
        ax.axhline(y=0, color='k', linewidth=0.7)

    # set axis limits
    ax.set_xlim(reltime_range[0] - 1, reltime_range[1] + 1)

    # add legend
    #ax.legend(bbox_to_anchor=(1.02, 0.91), fontsize=8)
    #ax.legend(loc="upper right", fontsize=8)
    #ax.legend(loc="best", fontsize=8)

    # axis labels
    ax.set_ylabel("Vel [m/s]")

    ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)

    return

if __name__ == "__main__":

    # NOTE: moving average is implemented

    # input parameters
    nvel_min=20
    lat_min = 50
    #lat_range=[54, 59]
    lat_range=[55, 59]

    lat_avg = False   # Do latitudinal average if set to True
    #NOTE: if lat_avg is True, sampling_method can take "mean" or "median"
    # If lat_avg is False,  sampling_method can take "mean" or "median" or None
    #sampling_method="median"
    sampling_method="mean"
    #sampling_method=None
    reltime_range=[-30, 30]
    del_mlt = 0.4
    reltime_resolution=7
    del_reltime = 1
    mlt_width=1.
    gazmc_span_minlim=30
    vel_mag_err_maxlim=0.1
    vel_mag_maxlim=200.
    fit_by_bmazm=False
    fit_by_losvel_azm=True
    ftype = "fitacf"
    coords = "mlt"
    #weighting = None 
    weighting = "std"
    dbdir="../data/sqlite3/"
    #IMF_turning = "northward"
    IMF_turning = "southward"
    add_err_bar = False

    input_table = "master_cosfit_superposed_" + IMF_turning +\
                  "_mltwidth_" + str(int(mlt_width)) +\
                  "_res_" + str(reltime_resolution) + "min" +\
                  "_deltm_" + str(del_reltime) + "min" +\
                  "_sixrads_quiet_v2"
                  #"_sixrads_quiet"

#    mlt_ranges=[[-3-del_mlt, -3+del_mlt], [-1-del_mlt, -1+del_mlt],
#                [0-del_mlt,   0+del_mlt], [0-del_mlt,   0+del_mlt],
#                [1-del_mlt,   1+del_mlt], [3-del_mlt,   3+del_mlt]]


#    mlt_ranges=[[-4-del_mlt, -4+del_mlt], [-2-del_mlt, -2+del_mlt],
#                [0-del_mlt,   0+del_mlt], [0-del_mlt,   0+del_mlt],
#                [2-del_mlt,   2+del_mlt], [4-del_mlt,   4+del_mlt]]

#    ylim_list = [[-180, 180], [-100, 100],
#                 [-100, 100], [-50, 50],
#                 [-100, 100], [-100, 100]]


#    mlt_ranges=[[-4-del_mlt, -4+del_mlt], [-2-del_mlt, -2+del_mlt], [-1-del_mlt,   -1+del_mlt],  
#                [0-del_mlt,   0+del_mlt],
#                [1-del_mlt,   1+del_mlt], [2-del_mlt,   2+del_mlt], [4-del_mlt,   4+del_mlt]]

    mlt_ranges=[[-4-del_mlt, -4+del_mlt], [-3-del_mlt, -3+del_mlt], [-2-del_mlt, -2+del_mlt],
                [-1-del_mlt, -1+del_mlt], [0-del_mlt,   0+del_mlt], [1-del_mlt,   1+del_mlt],
                [2-del_mlt,   2+del_mlt], [3-del_mlt,   3+del_mlt], [4-del_mlt,   4+del_mlt]]



    #veldir_list = ["zonal", "zonal", "zonal", "meridional", "zonal", "zonal"]
    veldir = "zonal"
    #veldir = "meridional"
    veldir_list = [veldir] * len(mlt_ranges)

    if veldir == "zonal":
        ylim_list = [[-160, 50]]  * len(mlt_ranges)
    else:
        #ylim_list = [[-80, 80]]  * len(mlt_ranges)
        ylim_list = [[-40, 40]]  * len(mlt_ranges)


    glatc_list = np.arange(lat_range[1], lat_range[0], -1) - 0.5 

    #fig, axes = plt.subplots(nrows=len(mlt_ranges), ncols=1, sharex=True, figsize=(6, 15))
    fig, axes = plt.subplots(nrows=3, ncols=3, sharex=True, sharey=True, figsize=(15, 6))
    axes = [x[0] for x in axes.T.reshape((9,1))]
    fig.subplots_adjust(hspace=0.3, wspace=0.2)


    for i, mlt_range in enumerate(mlt_ranges): 
        ax = axes[i]
        center_mlt = round((mlt_range[0] + mlt_range[0])/2., 0) % 24
        veldir = veldir_list[i]
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
            title = "Velocity Magnitude, MLT = " + str(round(center_mlt))
        else:
            title = veldir[0].upper()+veldir[1:] + " Velocities, MLT = " + str(round(center_mlt))
        vel_vs_reltime(ax, data_dict, mlt_range=mlt_range, IMF_turning=IMF_turning,
                       veldir=veldir, sampling_method=sampling_method,
                       lat_avg=lat_avg, glatc_list=glatc_list, title=title, add_err_bar=add_err_bar)
        ax.yaxis.set_major_locator(MultipleLocator(base=40))

        if veldir == "all":
            ax.set_ylim([0, 200])
        else:
            ax.set_ylim(ylim_list[i])

    # set axis label
    axes[-1].set_xlabel("Time [min]")
    axes[-1].xaxis.set_major_locator(MultipleLocator(base=5))

    # save the fig
    #fig_dir = "/home/muhammad/Dropbox/tmp/velcomp_vs_reltime/"
    fig_dir = "../plots/velcomp_vs_reltime/sixrads_quiet/"
    #fig_name = "line_plot"
    if lat_avg:
        tmp_txt = "_lat_avg"
    else:
        tmp_txt = ""
    fig_name = veldir + "_" + IMF_turning + tmp_txt + "_lat" + str(lat_range[0]) +\
               "_to_lat" + str(lat_range[1]) +\
               "_mltwidth_" + str(int(mlt_width)) +\
               "_res_" + str(reltime_resolution) + "min" + \
               "_deltm_" + str(del_reltime) + "min" +\
               "_nvel_min_" + str(nvel_min) +\
               "_velmag_err_"  + str(vel_mag_err_maxlim) + "_v2"

    fig.savefig(fig_dir + fig_name + ".png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    #plt.show()


