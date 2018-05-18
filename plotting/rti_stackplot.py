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


def plot_rti(ax, event_dtm, rad,
             bmnum=7, mag_bmazm=None,
             stable_interval=30, ftype="fitacf",
             mag_latc_range=[53, 62], vel_maxlim=500,
             cmap="jet", norm=None,
             IMF_turning="southward", db_name = None,
             dbdir = "../data/sqlite3/"):

    """ Makes an range-time plot of a radar beam for certain IMF turning period

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
    if bmnum is None:
        if mag_bmazm is None:
            mag_bmazm = 30

        # Find bmnum that corresponds to mag_bmazm
        bmnum = find_bmnum(rad, stm, etm, mag_bmazm=mag_bmazm,
                           bmazm_diff=3., db_name=db_name,
                           dbdir=dbdir)

    ccoll = None
    if bmnum is not None:
        # load data to a dataframe
        command = "SELECT vel, mag_latc, rad_mlt, bmnum, datetime FROM {tb} " + \
                  "WHERE datetime BETWEEN '{stm}' AND '{etm}' "+\
                  "AND bmnum={bmnum}"
        command = command.format(tb=rad, stm=stm, etm=etm, bmnum=bmnum)
        df = pd.read_sql(command, conn)
        if not df.empty:
            for i, rw in df.iterrows():
                vl = json.loads(rw.vel)
                lat = json.loads(rw.mag_latc)

                # Select data between the mag_latc_range
                vels_tmp = np.array([vl[i] for i in range(len(vl))\
                        if (lat[i] >= mag_latc_range[0] and lat[i] <= mag_latc_range[1])])
                lats_tmp = np.array([lat[i] for i in range(len(lat))\
                        if (lat[i] >= mag_latc_range[0] and lat[i] <= mag_latc_range[1])])

                # Remove outliers
                #vels_tmp = vels_tmp[np.where(vels_tmp-vels_tmp.mean()<=2*vels_tmp.std())]

                # Plot the data
                dtm_tmp = pd.to_datetime(rw.datetime)
                relative_time = (dtm_tmp-event_dtm).total_seconds()/60.
                xs = [relative_time]*len(lats_tmp)
                ys = lats_tmp
                ccoll = ax.scatter(xs, ys, s=4.0, zorder=1,
                                   marker="s", c=vels_tmp,
                                   linewidths=.5, edgecolors='face',
                                   cmap=cmap, norm=norm)
            rad_mlt_loc = round(df.rad_mlt.as_matrix()[0]/15.,1)
            lbl = rad + ",b" + str(bmnum) + "\nMLT=" + str(rad_mlt_loc)
            ax.annotate(lbl, xy=(0.90, 0.1), xycoords="axes fraction", fontsize=8)
    ax.set_ylabel("MLAT", fontsize=8)
    ax.set_ylim([mag_latc_range[0]-1, mag_latc_range[1]+1])
    ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)

    # Close conn
    conn.close()

    return ccoll

def add_cbar(fig, mappable, label="Velocity [m/s]", cax=None,
             ax=None, shrink=0.65, title_size=14,
             ytick_label_size=12):

    # add color bar
    if cax:
        cbar=fig.colorbar(mappable, ax=ax, cax=cax,
                          orientation="vertical", drawedges=False)
    else:
        cbar=fig.colorbar(mappable, ax=ax, cax=cax, shrink=shrink,
                          orientation="vertical", drawedges=False)

#    #define the colorbar labels
#    l = []
#    for i in range(0,len(bounds)):
#        if i == 0 or i == len(bounds)-1:
#            l.append(' ')
#            continue
#        l.append(str(int(bounds[i])))
#    cbar.ax.set_yticklabels(l)
    cbar.ax.tick_params(axis='y',direction='in')
    cbar.set_label(label)

    #set colorbar ticklabel size
    for ti in cbar.ax.get_yticklabels():
        ti.set_fontsize(ytick_label_size)
    cbar.set_label(label, size=title_size)
    cbar.extend='max'



if __name__ == "__main__":

    from matplotlib.colors import BoundaryNorm, Normalize

    stable_interval=30
    #IMF_turning = "southward"
    IMF_turning = "northward"
    cmap="jet_r"
    norm = Normalize(vmin=-100,vmax=100)

    #event_dtm = dt.datetime(2014, 12, 16, 14, 2)
    #event_dtm = dt.datetime(2015, 1, 5, 3, 44)
    event_dtm = dt.datetime(2014, 1, 1, 8, 0)

    #rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]
    #rads = ["cve", "cvw", "ade", "adw"] 
    rads = ["wal", "bks", "fhe", "fhw"]
    #rads = ["fhe", "fhw"]
    #rads = ["cve", "cvw"]
    #rads = ["cve", "ade"]

    #bmnums = range(0, 15, 2)
    #bmnums = range(1, 16, 2)
    #bmnums = range(0, 24, 3)
    bmnums = [1, 7, 13, 19]
    fig, axes = plt.subplots(nrows=len(bmnums)*len(rads), ncols=1, figsize=(6,12),
                             sharex=True)

    # Plot LOS velocity 
    mappable = None
    for k, rad in enumerate(rads):
        for i, bmnum in enumerate(bmnums):
            ax = axes[(len(bmnums))*k+i]
            losvel_mappable = plot_rti(ax, event_dtm, rad,
                                       bmnum=bmnum, mag_bmazm=None,
                                       stable_interval=stable_interval,
                                       ftype="fitacf", mag_latc_range=[53, 65],
                                       cmap=cmap, norm=norm,
                                       IMF_turning=IMF_turning, db_name = None,
                                       dbdir = "../data/sqlite3/")
            if losvel_mappable:
                mappable = losvel_mappable
    axes[0].set_title(event_dtm.strftime("%m/%d/%Y  %H:%M"))
    axes[-1].set_xlabel("Time [min]")

    # Add colorbar for LOS Vel.
    if mappable:
        fig.subplots_adjust(right=0.8)
        cbar_ax = fig.add_axes([0.85, 0.30, 0.03, 0.4])
        #fig.colorbar(im, cax=cbar_ax)
        add_cbar(fig, mappable, label="Velocity [m/s]", cax=cbar_ax,
                 ax=None, title_size=10, ytick_label_size=10)

##############################
    from superpose_single_event_losvel import plot_imf, plot_aualae

    fig2, axes = plt.subplots(nrows=2, ncols=1, figsize=(6,8),
                             sharex=True)
    ax_imf, ax_ae = axes
    ax_theta = None
    # Plot IMF
    plot_imf(event_dtm, ax_imf=ax_imf, ax_theta=ax_theta,
             stable_interval=stable_interval, ylim=[-12, 12],
             dbdir="../data/sqlite3/", db_name="gmi_imf.sqlite",
             table_name="IMF")

    # Plot AU, AL
    plot_aualae(event_dtm, ax_ae, plot_ae=False,
                stable_interval=stable_interval, ylim_au=[0, 500],
                ylim_al=[-500, 0], ylabel_fontsize=9,
                marker='.', linestyle='--')
##############################

    #plt.show()
    fig.savefig("/home/muhammad/Dropbox/tmp/fig1.png", dpi=200, bbox_inches="tight")
    fig2.savefig("/home/muhammad/Dropbox/tmp/fig2.png",dpi=200,  bbox_inches="tight")
