import sys
sys.path.append("../data/")
from create_event_list import create_event_list
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import json
import matplotlib.pyplot as plt
plt.style.use("ggplot")

def find_bmnum(rad, stm, etm, mag_bmazm=45,
               bmazm_diff=3.,
               db_name="sd_gridded_los_data_fitacf.sqlite",
               dbdir = "../data/sqlite3/"):

    """ Finds beam number that corresponds to a certain mag_bmazm"""

    # make a connection
    conn = sqlite3.connect(dbdir + db_name,
                           detect_types = sqlite3.PARSE_DECLTYPES)

    # load data to a dataframe
    command = "SELECT bmnum, mag_bmazm FROM {tb} " + \
              "WHERE datetime BETWEEN '{stm}' AND '{etm}' "
    command = command.format(tb=rad, stm=stm, etm=etm)
    df = pd.read_sql(command, conn)

    df.loc[:, "bmazm_diff"] = np.abs(df.mag_bmazm - mag_bmazm%360)
    df = df.loc[df.bmazm_diff <= bmazm_diff, :]
    df.sort_values("bmazm_diff", inplace=True)

    try:
        bmnum = df.bmnum.as_matrix()[0]
    except:
        bmnum = None

    # Close conn
    conn.close()

    return bmnum

def plot_superposed_los(ax, event_dtm,
                        rads, stable_interval=30,
                        ftype="fitacf",
                        mag_bmazms=None,
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
        if mag_bmazms is None:
            mag_bmazm = 30
        else:
            mag_bmazm = mag_bmazms[i]
        # Find bmnum that corresponds to mag_bmazm
        bmnum = find_bmnum(rad, stm, etm, mag_bmazm=mag_bmazm,
                           bmazm_diff=3.,
                           db_name="sd_gridded_los_data_fitacf.sqlite",
                           dbdir = "../data/sqlite3/")

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
                vels_tmp = vels_tmp[np.where(vels_tmp-vels_tmp.mean()<=2*vels_tmp.std())]

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
            xs = df.relative_time
            ys = [x if np.abs(x) < vel_maxlim else np.nan for x in df.loc[:, method+"_vel"]]
            ax.plot(xs, ys, marker='.', linestyle="-", label=lbl)
    ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)
    ax.legend()
    ax.set_title(event_dtm.strftime("%m/%d/%Y  %H:%M"))

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

    fig, ax = plt.subplots()
#    IMF_turning = "southward"
#    #IMF_turning = "northward"
#    stable_interval=30
#    plot_superposed_imf(ax, stable_interval=stable_interval,
#                        IMF_turning=IMF_turning)

#    stm = dt.datetime(2014, 12, 16, 13, 30)
#    etm = dt.datetime(2014, 12, 16, 15, 0)
#    rad = "fhw"
#    bmnum = find_bmnum(rad, stm, etm, mag_bmazm=-30,
#                    bmazm_diff=3.,
#                    db_name="sd_gridded_los_data_fitacf.sqlite",
#                    dbdir = "../data/sqlite3/")

    #rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]
    rads = ["fhe", "fhw", "cve", "cvw"]
    #rads = ["cve", "cvw", "ade", "adw"]
    azm_e = 20; azm_w = -20
#    mag_bmazms=[azm_e, azm_w, azm_e, azm_w,
#                azm_e, azm_w, azm_e, azm_w]
    mag_bmazms=[azm_e, azm_w, azm_e, azm_w]

    #event_dtm = dt.datetime(2014, 12, 16, 14, 2)
    #event_dtm = dt.datetime(2013, 11, 16, 7, 25)
    event_dtm = dt.datetime(2013, 11, 16, 7, 47)
    df_dict = plot_superposed_los(ax, event_dtm,
                                  rads, stable_interval=30,
                                  ftype="fitacf",
                                  mag_bmazms=mag_bmazms,
                                  mag_latc_range=[55, 59],
                                  method="mean", vel_maxlim=200,
                                  IMF_turning="northward",
                                  db_name = None,
                        dbdir = "../data/sqlite3/")

    ax.set_ylim([-150, 150])
    plt.show()
