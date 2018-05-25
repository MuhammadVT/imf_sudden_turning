import matplotlib
matplotlib.use('Agg')
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import logging


def plot_cosfit(ax, latc, ltc, master_table, cosfit_table,
                relative_time=-30, reltime_resolution=2, mlt_width=1., 
                ftype="fitacf", coords="mlt", 
                fit_by_bmazm=False, fit_by_losvel_azm=True,
                db_name=None, dbdir="../data/sqlite3/",
                sqrt_weighting=False, add_errbar=False):

    """ plots a the cosfit results for a give latc-ltc grid for a given relative_time
    Parameters
    ----------
    master_table : str
        A table name in db_name db
    cosfit_table : str
        A table name in db_name db
    relative_time : int
        The relative time of interest
    reltime_resolution : int
        The time resolution (in minutes) of the fitted data
        e.g.,  for start_reltime=-30 and reltime_resolution=2, the relative time range
        for a cos-fitting procedure would be [-30, -29]
    mlt_width : float
        The width of MLT region within which cosine fitting will be performed.
        i.e., for a point at 1 MLT, the points within 1  +/- 0.5*mlt_width will be
        used to fit a cosine curve.
    fit_by_bmazm : bool (Default to False)
        If set to true, the cosfitting would be done on losvel-vs-magbmazm.
    fit_by_losvel_azm : bool (Default to True)
    db_name : str, default to None
        Name of the master db
    ftype : str
        SuperDARN file type
    coords : str
        Coordinates in which the binning process took place.
        Default to "mlt, can be "geo" as well.
    sqrt_weighting : bool
        if set to False, the fitted vectors that are produced by equality weighting
        the number of points within each azimuthal bin will be retrieved.

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
    if sqrt_weighting:
        cosfit_table = cosfit_table
    else:
        cosfit_table = cosfit_table

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

    # Find points that best matches the user input latc, ltc.
    command = "SELECT {glatc}, {gltc} FROM {tb} WHERE relative_time={relative_time}"
    command = command.format(tb=cosfit_table, glatc=col_glatc,
                             gltc = col_gltc, relative_time=relative_time)
    cur.execute(command)
    rows = cur.fetchall()
    all_lats = np.array([x[0] for x in rows])
    matching_lats_idx = np.where(all_lats==latc)
    all_lts = np.array([x[1] for x in rows])
    possible_lts = all_lts[matching_lats_idx]
    ltc_idx = (np.abs(possible_lts - ltc)).argmin()
    ltc = round(possible_lts[ltc_idx],2)
    
    # Find the AZM and LOS info
    sreltm = relative_time
    ereltm = relative_time + (reltime_resolution-1)
    gltc_left = ltc - mlt_width/2.*15.
    gltc_right = ltc + mlt_width/2.*15.
    if gltc_right >=360 or gltc_left < 0:
        gltc_left =  gltc_left % 360
        where_clause = "AND (({gltc} BETWEEN {gltc_left} AND 360) or ({gltc} BETWEEN 0 AND {gltc_right})) "
    else:
        where_clause = "AND ({gltc} BETWEEN {gltc_left} AND {gltc_right}) "
    command = "SELECT vel, {gazmc}, rad FROM {tb2} " +\
              "WHERE {glatc}={lat} " + where_clause + \
              "AND (relative_time BETWEEN {sreltm} AND {ereltm}) " +\
              "ORDER BY {gazmc}"
    command = command.format(tb2=master_table, glatc=col_glatc, gltc=col_gltc,
                             gltc_left=gltc_left, gltc_right=gltc_right,
                             sreltm=sreltm, ereltm=ereltm,
                             gazmc=col_gazmc, lat=latc)

    cur.execute(command)
    rows = cur.fetchall()
    los_vel = -np.array([x[0] for x in rows])
    azm = np.array([x[1] for x in rows])
    azm = [x if x <= 180 else x-360 for x in azm]
    rad = [x[2] for x in rows]

    # select the cosine fitting results from db
    command = "SELECT vel_count, vel_mag, vel_mag_err, vel_dir, " + \
              "vel_dir_err FROM {tb} WHERE {glatc}={lat} " +\
              "AND {gltc}={lt} AND relative_time={relative_time}"
    command = command.format(tb=cosfit_table, glatc=col_glatc,
                             gltc = col_gltc, lat=latc, lt=ltc,
                             relative_time=relative_time)
    cur.execute(command)
    row = cur.fetchall()[0]
    vel_count = row[0]
    vel_mag = -row[1]
    vel_mag_err = row[2]
    vel_dir = row[3]
    vel_dir_err = row[4]

    # close db connection
    conn.close()

    # plot the LOS data
    weight = 1
    ax.scatter(azm, los_vel, marker='o',c='k', s=0.6*weight,
               edgecolors="face", label="LOS Vel.")

    # add error bars to LOS vels
    if add_errbar:
        pass
#        ax.errorbar(azm, los_vel, yerr=vel_std, capsize=1, mfc='k',
#                fmt='o', ms=2, elinewidth=.5, mec='k', ecolor="k")

    # plot the cosfit curve
    #x_fit = np.arange(0, 360, 1)
    x_fit = np.arange(-180, 180, 0.01)
    y_fit = vel_mag * np.cos(np.deg2rad(x_fit) - np.deg2rad(vel_dir))
    ax.plot(x_fit, y_fit, 'y', linewidth=1, label="Fit Line")

    # mark the peak position
    ind_max = np.argmax(y_fit)
    y_max = y_fit[ind_max]
    x_max = x_fit[ind_max]
    fsz = 5
    ax.scatter(x_max, y_max, c='r', edgecolors="face", marker = '*', s = 50, label="Fitted Vel.", zorder=5)
    ax.annotate('vel=' + '{0:.01f}'.format(y_max) , xy = (0.02, 0.88), xycoords='axes fraction',\
       horizontalalignment='left', verticalalignment='bottom', fontsize=fsz) 
    ax.annotate('azm=' + '{0:.01f}'.format(x_max) +'$^\circ$' , xy = (0.015, 0.78), xycoords='axes fraction',\
       horizontalalignment='left', verticalalignment='bottom', fontsize=fsz) 
    
    # fitting error values
#    ax.annotate('vel_std=' + '{0:.01f}'.format(vel_mag_err) , xy = (0.02, 0.74), xycoords='axes fraction',\
#       horizontalalignment='left', verticalalignment='bottom', fontsize=fsz) 
#    ax.annotate('azm_std=' + '{0:.01f}'.format(vel_dir_err) +'$^\circ$' , xy = (0.02, 0.66), xycoords='axes fraction',\
#            horizontalalignment='left', verticalalignment='bottom', fontsize=fsz) 
    
    # put labels
    ax.set_title("Time = " + str(relative_time) +\
                 ", MLat = " + str(latc) + ", MLT = " + str(round(ltc/15., 1)))
    ax.set_xlabel("Azimuth [$^\circ$]")
    ax.set_ylabel("Velocity [m/s]")

    return
    
if __name__ == "__main__":

    # input parameters
    relative_time=-30
    reltime_resolution=2
    mlt_width=1.
    fit_by_bmazm=False
    fit_by_losvel_azm=True
    ftype = "fitacf"
    coords = "mlt"
    sqrt_weighting = False
    dbdir="../data/sqlite3/"

    master_table = "master_superposed_epoch"
    cosfit_table = "master_cosfit_superposed_mltwidth_" + str(int(mlt_width)) +\
                   "_res_" + str(reltime_resolution) + "min"

    fixed_lat = True
    fixed_lt = True
    # Plot points at a given latitude
    if fixed_lat:
        # points of interest
        #latc, ltc = 46.5, 0
        #latc_list = [x+0.5 for x in range(42, 50)]
        latc_list = [x+0.5 for x in range(54, 61)]
        ltc_list = range(270, 360, 15) + range(0, 90, 15)
        
        # plotting
        for latc in latc_list:
            # create a figure
            fig, axes = plt.subplots(4,3, sharex=True, sharey=True)
            plt.subplots_adjust(hspace=0.4)
            axes = [x for l in axes for x in l]

            fig_dir = "../plots/cosfit_plot/"
            #fig_name = rads_txt + "_" + relative_time + "_cosfit_mlat"+str(latc) + \
            #           "_mlt" + str(round(ltc/15., 2))
            fig_name = "reltime_" + str(relative_time) + "_cosfit_mlat"+str(latc)

            for i, ltc in enumerate(ltc_list):
                ax = axes[i]
                plot_cosfit(ax, latc, ltc, master_table, cosfit_table,
                            relative_time=relative_time, reltime_resolution=reltime_resolution,
                            mlt_width=mlt_width, ftype=ftype, coords="mlt", 
                            fit_by_bmazm=fit_by_bmazm, fit_by_losvel_azm=fit_by_losvel_azm,
                            db_name=None, dbdir=dbdir,
                            sqrt_weighting=sqrt_weighting, add_errbar=False)

                ax.set_xlim([-180, 180])
                ax.set_ylim([-100, 100])

                # change the font
                for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                             ax.get_xticklabels() + ax.get_yticklabels()):
                    item.set_fontsize(6)
            for ax in axes:
                ax.set_xlabel("")
                ax.set_ylabel("")
            
            # add legend
            axes[5].legend(bbox_to_anchor=(1, 0.7), fontsize=5, frameon=False)

            # save the plot
	    #plt.figtext(0.5, 0.95, kp_text_dict[kp_text][1:], ma="center")
            fig.savefig(fig_dir + fig_name + ".png", dpi=300)
            plt.close(fig)

    # Plot points at a given local time
    if fixed_lt:
        # points of interest
        #latc, ltc = 46.5, 0
        #latc_list = [x+0.5 for x in range(42, 54)]
        latc_list = [x+0.5 for x in range(52, 64)]
        #ltc_list = range(270, 360, 15) + range(0, 90, 15)
        ltc_list = range(270, 360, 30) + range(0, 120, 30)
        
        # plotting
        for ltc in ltc_list:
            # create a figure
            fig, axes = plt.subplots(4,3, sharex=True, sharey=True)
            plt.subplots_adjust(hspace=0.4)
            axes = [x for l in axes for x in l]

            fig_dir = "../plots/cosfit_plot/"
            fig_name = "reltime_" + str(relative_time) + "_mlt" + str(round(ltc/15., 0))

            for i, latc in enumerate(latc_list):
                ax = axes[i]
                plot_cosfit(ax, latc, ltc, master_table, cosfit_table,
                            relative_time=relative_time, reltime_resolution=reltime_resolution,
                            mlt_width=mlt_width, ftype=ftype, coords="mlt", 
                            fit_by_bmazm=fit_by_bmazm, fit_by_losvel_azm=fit_by_losvel_azm,
                            db_name=None, dbdir=dbdir,
                            sqrt_weighting=sqrt_weighting, add_errbar=False)

                ax.set_xlim([-180, 180])
                ax.set_ylim([-100, 100])

                # change the font
                for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                             ax.get_xticklabels() + ax.get_yticklabels()):
                    item.set_fontsize(6)
            for ax in axes:
                ax.set_xlabel("")
                ax.set_ylabel("")
            
            # add legend
            axes[5].legend(bbox_to_anchor=(1, 0.7), fontsize=5, frameon=False)

            # save the plot
	    #plt.figtext(0.5, 0.95, kp_text_dict[kp_text][1:], ma="center")
            fig.savefig(fig_dir + fig_name + ".png", dpi=300)
            plt.close(fig)

