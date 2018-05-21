from davitpy.utils.coordUtils import coord_conv
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import json
import matplotlib.pyplot as plt
import sys
sys.path.append("../data/")
from create_event_list import create_event_list
from funcs import add_cbar
#plt.style.use("ggplot")

def plot_tec(ax, dtm, mag_latc_range=[53, 62], mltc_range=[-6, 6],
             t_c_alt=0., cmap="gist_gray_r", scatter=False, norm=None,
             db_name=None, dbdir="../data/sqlite3/"):

    """ Makes a TEC plot for a given dtm

    Parameters
    ----------

    """
    
    # Set the minutes a factor of 5
    dtm = dtm.replace(minute=5*int(dtm.minute/5))

    if db_name is None:
         db_name = "med_filt_tec.sqlite"

    # make a connection
    conn = sqlite3.connect(dbdir + db_name,
                           detect_types = sqlite3.PARSE_DECLTYPES)

    # load data to a dataframe
    table_name = "med_filt_tec"
    command = "SELECT mlat, mlon, med_tec FROM {tb} " +\
	      "WHERE datetime = '{dtm}'"
    command = command.format(tb=table_name, dtm=dtm)
    df = pd.read_sql(command, conn)

    # Filter the data by MLAT
    df = df.loc[(df.mlat>= mag_latc_range[0]) & (df.mlat <= mag_latc_range[1]), :]

    # Plot the data
    ccoll = None
    if not df.empty:
        # convert from mag to mlt coords
        lats = df.mlat.as_matrix() 
        lons = df.mlon.as_matrix() 
        lts, lats = coord_conv(lons, lats, "mag", "mlt", altitude=t_c_alt,
                               date_time=dtm)
        lts = [(round(x, 1))%360 for x in lts]
        lats = [round(x, 1) for x in lats]

        # Make MLT between -180 to 180
        lts = [x if x <=180 else x-360 for x in lts]
        df["mlt"] = lts
        df["mlat"] = lats

        # Filter the data by MLT
        df = df.loc[(df.mlt>= mltc_range[0]*15.) & (df.mlt <= mltc_range[1]*15.), :]
        df = df.sort_values("mlt")

        # Construct arrays
        if scatter:
            xs = df.mlt.as_matrix()
            ys = df.mlat.as_matrix()
            cs = df.med_tec.as_matrix()
        else:
            xs = np.arange(df.mlt.min(), df.mlt.max()+2, 2)    # in degrees
            ys = np.arange(mag_latc_range[0], mag_latc_range[1]+1)
            cs = np.ones((len(xs), len(ys))) * np.nan
            for i, x in enumerate(xs):
                for j, y in enumerate(ys):
                    df_tmp = df.loc[(np.isclose(df.mlt, x)) & (np.isclose(df.mlat,y))]
                    if not df_tmp.empty:
                        cs[i, j] = df_tmp.med_tec.as_matrix()[0]

        # Convert MLT from degrees to hours
        xs = xs / 15.

        # Plot the data
        if scatter:
            ccoll = ax.scatter(xs, ys, s=30.0, zorder=1,
                               marker="s", c=cs,
                               linewidths=.5, edgecolors='face',
                               cmap=cmap, norm=norm)
        else:
            X, Y = np.meshgrid(xs, ys)
            Z = np.ma.masked_where(np.isnan(cs.T), cs.T)
            ccoll = ax.pcolormesh(X, Y, Z, edgecolor=None, cmap=cmap, norm=norm)

        # Annotate the starting MLT location of the radar
#        rad_mlt_loc = round(df.rad_mlt.as_matrix()[0]/15.,1)
#        lbl = rad + ",b" + str(bmnum) + "\nMLT=" + str(rad_mlt_loc)
#        ax.annotate(lbl, xy=(0.90, 0.1), xycoords="axes fraction", fontsize=8)
    ax.set_ylabel("MLAT", fontsize=10)
    ax.set_ylim([mag_latc_range[0], mag_latc_range[1]])
    ax.set_xlim([mltc_range[0], mltc_range[1]])
    #ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)

    # Close conn
    conn.close()

    return ccoll


if __name__ == "__main__":

    from matplotlib.colors import BoundaryNorm, Normalize

    stable_interval=30
    IMF_turning = "southward"
    #IMF_turning = "northward"
    cmap="gist_gray_r"
    norm = Normalize(vmin=0,vmax=10)

    #event_dtm = dt.datetime(2014, 12, 16, 14, 2)
    #event_dtm = dt.datetime(2013, 2, 21, 5, 34)
    #event_dtm = dt.datetime(2014, 1, 1, 8, 0)
    event_dtm = dt.datetime(2014, 2, 3, 7, 20)

    # Create event list
    df_turn = create_event_list(IMF_turning=IMF_turning)
    df_turn = df_turn.loc[df_turn.datetime==event_dtm, :]

    # Find the convection respond time
    lag_time = df_turn.lag_time.iloc[0]
    event_dtm_lagged = event_dtm + dt.timedelta(seconds=60. * lag_time)

    # Construct stm and etm
    stm = event_dtm_lagged - dt.timedelta(seconds=60. * stable_interval)
    stm = stm.replace(minute=5*int(stm.minute/5))
    etm = event_dtm_lagged + dt.timedelta(seconds=60. * stable_interval)
    etm = etm.replace(minute=5*(int(etm.minute/5)+1))
    
    # datetimes for a single event
    dtms = [stm + dt.timedelta(seconds=60. * i) for i in range(10, 70, 10)] 

    # Plot LOS velocity 
    fig, axes = plt.subplots(nrows=len(dtms), ncols=1, figsize=(6,12),
                             sharex=True)
    mappable = None
    for k, dtm in enumerate(dtms):
        ax = axes[k]
        tec_mappable = plot_tec(ax, dtm, mag_latc_range=[53, 65], mltc_range=[-6, 6],
                                t_c_alt=0., cmap=cmap, norm=norm, scatter=False,
                                db_name = "med_filt_tec.sqlite",
                                dbdir = "../data/sqlite3/")
        ax.set_title(dtm.strftime("%m/%d/%Y  %H:%M"), fontsize=10)
        if tec_mappable:
            mappable = tec_mappable
    axes[-1].set_xlabel("MLT")

    # Add colorbar for LOS Vel.
    if mappable:
        fig.subplots_adjust(right=0.8)
        cbar_ax = fig.add_axes([0.85, 0.30, 0.03, 0.4])
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
    ax_imf.set_title(event_dtm.strftime("%m/%d/%Y  %H:%M"))
##############################

    #plt.show()
    fig.savefig("/home/muhammad/Dropbox/tmp/fig1.png", dpi=200, bbox_inches="tight")
    fig2.savefig("/home/muhammad/Dropbox/tmp/fig2.png",dpi=200,  bbox_inches="tight")
