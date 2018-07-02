import sys
sys.path.append("../data/")
from create_event_list import create_event_list
from funcs import find_bmnum
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import json
import matplotlib.pyplot as plt
plt.style.use("ggplot")

def plot_imf(event_dtm, ax_imf=None, ax_theta=None,
             stable_interval=30, ylim=[-10, 10],
             dbdir="../data/sqlite3/", db_name="gmi_imf.sqlite",
             table_name="IMF"):

    """ Plots IMF centered at event_dtm with 2*stable_interval
        minutes of interval"""

    # make a connection
    conn = sqlite3.connect(dbdir + db_name,
                           detect_types = sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()
    stm = event_dtm - dt.timedelta(seconds=60. * stable_interval)
    etm = event_dtm + dt.timedelta(seconds=60. * stable_interval)
    # load data to a dataframe
    command = "SELECT Bx, By, Bz, theta, datetime FROM {tb} " + \
              "WHERE datetime BETWEEN '{stm}' AND '{etm}' "
    command = command.format(tb=table_name, stm=stm, etm=etm)
    df = pd.read_sql(command, conn)
    dtms_tmp = pd.to_datetime(df.datetime)
    df.loc[:, "relative_time"] = [(x-event_dtm).total_seconds()/60. for x in dtms_tmp]

    # Plot IMF
    if ax_imf:
        ax_imf.plot(df.relative_time, df.Bz, "r", marker='.',
                    linestyle='--', label="Bz")
        ax_imf.plot(df.relative_time, df.By, "g", marker='.',
                    linestyle='--', label="By")
        ax_imf.axvline(x=0, color="r", linestyle="--", linewidth=1.)
        ax_imf.set_ylim(ylim)
        ax_imf.legend()

    if ax_theta:
        ax_theta.plot(df.relative_time, df.theta, "k", marker='.',
                      linestyle='--', label="Clock Angle")
        ax_theta.axvline(x=0, color="r", linestyle="--", linewidth=1.)
        ax_theta.set_ylim([0, 360])
        ax_theta.legend()

    return

def plot_aualae(event_dtm, ax_ae, lag_time=None, plot_ae=True,
                stable_interval=30, ylim_au=[0, 500],
                ylim_al=[-500, 0], ylabel_fontsize=9,
                marker='.', linestyle='--'):

    """ Plots AU, AL centered at event_dtm with 2*stable_interval
        minutes of interval"""

    import ae
	
    if lag_time is None:
	# Create event list
	df_turn = create_event_list(IMF_turning="all")
	df_turn = df_turn.loc[df_turn.datetime==event_dtm, :]

	# Find the convection respond time
	lag_time = df_turn.lag_time.iloc[0]
    event_dtm = event_dtm + dt.timedelta(seconds=60. * lag_time)

    stime = event_dtm - dt.timedelta(seconds=60. * stable_interval)
    etime = event_dtm + dt.timedelta(seconds=60. * stable_interval)

    # read AE data
    AE_list = ae.readAeWeb(sTime=stime,eTime=etime,res=1)
    AE = []
    AU = []
    AL = []
    AE_time = []
    for m in range(len(AE_list)):
        AU.append(AE_list[m].au)
        AL.append(AE_list[m].al)
        AE.append(AE_list[m].ae)
        AE_time.append(AE_list[m].time)

    # Select data of interest
    indx = [AE_time.index(x) for x in AE_time if (x>= stime) and (x<=etime)]
    AE_time = [AE_time[i] for i in indx]
    AE = [AE[i] for i in indx]
    AU = [AU[i] for i in indx]
    AL = [AL[i] for i in indx]

    # Convert AE_time to relative time
    relative_time = [(x-event_dtm).total_seconds()/60. for x in AE_time]

    # plot AU, AL, AE
    if plot_ae:
        indices = [AU, AL, AE]
        labels = ["AU", "AL", "AE"]
        colors = ["k", "g", "r"]
    else:
        indices = [AU, AL]
        labels = ["AU", "AL"]
        colors = ["k", "g"]

    for i, var in enumerate(indices):
        ax_ae.plot(relative_time, var, colors[i], marker=marker,
                   linestyle=linestyle, label=labels[i])
    ax_ae.axvline(x=0, color="r", linestyle="--", linewidth=1.)
    ax_ae.set_ylim([ylim_al[0], ylim_au[1]])
    ax_ae.locator_params(axis='y', nbins=6)
    ax_ae.legend()

    return

def plot_superposed_los(ax, event_dtm,
                        rads, lag_time=None, stable_interval=30,
                        ftype="fitacf", mag_bmazms=None,
                        bmnums=None,
                        mag_latc_range=[55, 59],
                        method="mean", vel_maxlim=500,
                        IMF_turning="southward",
                        db_name = None,
                        dbdir = "../data/sqlite3/"):

    """ Superimposes the LOS velocity at different beam look directions from
    several radars for IMF turnings

    Parameters
    ----------
    stable_interval : minutes
        The interval length before or after the IMF turning

    """

    if lag_time is None:
	# Create event list
	df_turn = create_event_list(IMF_turning=IMF_turning)
	df_turn = df_turn.loc[df_turn.datetime==event_dtm, :]

	# Find the convection respond time
	lag_time = df_turn.lag_time.iloc[0]
    event_dtm = event_dtm + dt.timedelta(seconds=60. * lag_time)

    # Construct stm and etm
    stm = event_dtm - dt.timedelta(seconds=60. * stable_interval)
    etm = event_dtm + dt.timedelta(seconds=60. * stable_interval)

    if db_name is None:
         db_name = "sd_gridded_los_data"+ "_" + ftype + ".sqlite"

    # make a connection
    conn = sqlite3.connect(dbdir + db_name,
                           detect_types = sqlite3.PARSE_DECLTYPES)
    df_dict = {}
    for i, rad in enumerate(rads):
        if bmnums is None:
            if mag_bmazms is None:
                mag_bmazm = 30
            else:
                mag_bmazm = mag_bmazms[i]

            # Find bmnum that corresponds to mag_bmazm
            bmnum = find_bmnum(rad, stm, etm, mag_bmazm=mag_bmazm,
                               bmazm_diff=3., db_name=db_name,
                               dbdir=dbdir)
        else:
            bmnum = bmnums[i]

        if bmnum:
            # load data to a dataframe
            command = "SELECT vel, mag_latc, rad_mlt, bmnum, datetime FROM {tb} " + \
                      "WHERE datetime BETWEEN '{stm}' AND '{etm}' "+\
                      "AND bmnum={bmnum}"
            command = command.format(tb=rad, stm=stm, etm=etm, bmnum=bmnum)
            df = pd.read_sql(command, conn)
            avgvel_lst = []
            for i, rw in df.iterrows():
                vl = json.loads(rw.vel)
                lat = json.loads(rw.mag_latc)
                vels_tmp = np.array([vl[i] for i in range(len(vl))\
                        if (lat[i] >= mag_latc_range[0] and lat[i] <= mag_latc_range[1])])

                # Remove outliers
                vels_tmp = vels_tmp[np.where(vels_tmp-vels_tmp.mean()<=1*vels_tmp.std())]

                # Find mean or median
                if method == "mean":
                    avg_vel = vels_tmp.mean()
                if method == "median":
                    avg_vel = vels_tmp.median()
                avgvel_lst.append(round(avg_vel,2)) 
            df.loc[:, method+"_vel"] = avgvel_lst
            df_dict[(rad, mag_bmazm)] = df

            # Plot the data
            dtms_tmp = pd.to_datetime(df.datetime)
            df.loc[:, "relative_time"] = [(x-event_dtm).total_seconds()/60. for x in dtms_tmp]
            lbl = "(" + rad + ",b" + str(bmnum) + "," + str(mag_bmazm) + ")"
            #lbl = "(" + rad + ",b" + str(bmnum) + ")"
            xs = df.relative_time
            ys = [x if np.abs(x) < vel_maxlim else np.nan for x in df.loc[:, method+"_vel"]]
            ax.plot(xs, ys, marker='.', linestyle="-", label=lbl)
    ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)
    #ax.legend()
    ax.legend(loc="center right", bbox_to_anchor=(1.15, 0.5),
              framealpha=0.3)

    # Close conn
    conn.close()

    return df_dict


def plot_superposed_gridded_los(ax, stable_interval=30,
                                filtered_interval=2.,
                                coords="mlt", ftype="fitacf",
                                glatc_range=[53, 58],
                                gazmc_range=[-10, 10],
                                gltc_range=[0, 1],
                                IMF_turning="southward",
                                table_name = "all_radars",
                                db_name = None,
                                dbdir = "../data/sqlite3/"):

    """ Superimposes the IMF turnings

    Parameters
    ----------
    stable_interval : minutes
        The interval length before or after the IMF turning

    """

    df_turn = create_event_list(IMF_turning=IMF_turning)

    if db_name is None:
         input_dbname = "sd_" + str(int(filtered_interval)) + "_min_median_" +\
                         coords + "_" + ftype + ".sqlite"
 
    # make a connection
    conn = sqlite3.connect(dbdir + db_name,
                           detect_types = sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    dtms = pd.to_datetime(df_turn.datetime)
    for dtm in dtms:
        stm = dtm - dt.timedelta(seconds=60. * stable_interval)
        etm = dtm + dt.timedelta(seconds=60. * stable_interval)
        # load data to a dataframe
        command = "SELECT Bx, By, Bz, theta, datetime FROM {tb} " + \
                  "WHERE datetime BETWEEN '{stm}' AND '{etm}' "
        command = command.format(tb=table_name, stm=stm, etm=etm)
        df = pd.read_sql(command, conn)
        dtms_tmp = pd.to_datetime(df.datetime)
        df.loc[:, "relative_time"] = [(x-dtm).total_seconds()/60. for x in dtms_tmp]
        ax.plot(df.relative_time, df.Bz)
        ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)

    ax.set_ylim([-10, 10])

    # Close conn
    conn.close()

    return

if __name__ == "__main__":

    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(6,12),
                             sharex=True)
    ax_imf, ax_ae, ax_los = axes
    ax_theta = None
    stable_interval=40
    #IMF_turning = "southward"
    IMF_turning = "northward"

    event_dtm = dt.datetime(2013, 2, 21, 5, 34)
    #event_dtm = dt.datetime(2015, 1, 5, 3, 45)
    #event_dtm = dt.datetime(2013, 5, 19, 5, 17)

    #rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]
    #rads = ["cve", "cvw", "ade", "adw"] 
    #rads = ["wal", "bks", "fhe", "fhw"]
    #rads = ["fhe", "fhw"]
    #rads = ["cve", "cvw"]
    #rads = ["cve", "ade"]
    rads = ["bks", "fhe", "fhw", "cve", "cvw"] 
    azm_e = 30; azm_w = -30
#    mag_bmazms=[azm_e, azm_w, azm_e, azm_w,
#                azm_e, azm_w, azm_e, azm_w]
    mag_bmazms=[-5, azm_e, azm_w, azm_e, azm_w]
#    bmnums = [9] * len(rads)
    bmnums = None
    mag_latc_range = [55, 59]
    #mag_latc_range = [58, 64]

    # Plot IMF
    plot_imf(event_dtm, ax_imf=ax_imf, ax_theta=ax_theta,
             stable_interval=stable_interval, ylim=[-12, 12],
             dbdir="../data/sqlite3/", db_name="gmi_imf.sqlite",
             table_name="IMF")

    # Plot AU, AL
    plot_aualae(event_dtm, ax_ae, plot_ae=True,
                stable_interval=stable_interval, ylim_au=[0, 500],
                ylim_al=[-500, 0], ylabel_fontsize=9,
                marker='.', linestyle='--')

    # Plot LOS velocity 
    df_dict = plot_superposed_los(ax_los, event_dtm,
                                  rads, stable_interval=stable_interval,
                                  ftype="fitacf",
                                  mag_bmazms=mag_bmazms,
                                  bmnums=bmnums,
                                  mag_latc_range=mag_latc_range,
                                  method="mean", vel_maxlim=200,
                                  IMF_turning=IMF_turning,
                                  db_name = None,
                                  dbdir = "../data/sqlite3/")

    ax_los.set_ylim([-100, 100])
    ax_imf.set_title(event_dtm.strftime("%m/%d/%Y  %H:%M"))

    # Save figure
    fig_dir = "../plots/superpose_single_event_losvel/"
    fig_name = "losvel_" + event_dtm.strftime("%Y%m%d.%H%M_") + IMF_turning + "_" + "_".join(rads) + "_MLAT" + "_".join([str(x) for x in mag_latc_range]) 
    fig.savefig(fig_dir + fig_name + ".png", dpi=200, bbox_inches="tight")
    plt.close(fig)
