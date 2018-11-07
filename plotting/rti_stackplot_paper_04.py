import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import json
from davitpy import pydarn
import matplotlib.pyplot as plt
plt.style.use("ggplot")
import sys
sys.path.append("../data/")
from build_event_database import build_event_database
from funcs import find_bmnum, add_cbar


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

        #ax_imf.legend()
        bbox_to_anchor=(1.2, 0.9)
        ax_imf.legend(loc="upper right", bbox_to_anchor=bbox_to_anchor,
                         fontsize=9, frameon=True)


    if ax_theta:
        ax_theta.plot(df.relative_time, df.theta, "k", marker='.',
                      linestyle='--', label="Clock Angle")
        ax_theta.axvline(x=0, color="r", linestyle="--", linewidth=1.)
        ax_theta.set_ylim([0, 360])
        ax_theta.legend()

    return


def plot_rti(ax, event_dtm, rad,
             bmnum=7, lag_time=None, mag_bmazm=None,
             stable_interval=30, ftype="fitacf",
             mag_latc_range=[53, 65], vel_maxlim=500,
             cmap="jet", norm=None, scatter_plot=True,
             IMF_turning="southward", db_name = None,
             dbdir = "../data/sqlite3/"):

    """ Makes an range-time plot of a radar beam for certain IMF turning period

    Parameters
    ----------
    stable_interval : minutes
        The interval length before or after the IMF turning

    """

    if lag_time is None:
	# Create event list
	df_turn_tmp = build_event_database(IMF_turning=IMF_turning)
	df_turn_tmp = df_turn_tmp.loc[df_turn_tmp.datetime==event_dtm, :]

	# Find the convection respond time
	lag_time = df_turn_tmp.lag_time.iloc[0]

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
        command = "SELECT vel, mag_latc, rad_mlt, bmnum, frang, nrang, rsep, slist, datetime " +\
                  "FROM {tb} " + \
                  "WHERE datetime BETWEEN '{stm}' AND '{etm}' "+\
                  "AND bmnum={bmnum} ORDER BY datetime"
        command = command.format(tb=rad, stm=stm, etm=etm, bmnum=bmnum)
        df = pd.read_sql(command, conn)

        if not df.empty:
            if scatter_plot:
                xs = []
                ys = []
                cs = []
            else:
                xs = np.zeros(2*len(df.datetime))
                # For geo or mag coords, get radar FOV lats/lons.
                rmax = df.nrang.max()
                fov_dtm = pd.to_datetime(df.datetime.tolist()[0])
                rsep = df.rsep.unique().tolist()[0]
                frang = df.frang.unique().tolist()[0]
                site = pydarn.radar.network().getRadarByCode(rad) \
                       .getSiteByDate(fov_dtm)
                myFov = pydarn.radar.radFov.fov(site=site, ngates=rmax,
                                                nbeams=site.maxbeam,
                                                rsep=rsep, frang=frang, coords="mag",
                                                coord_alt=300., date_time=fov_dtm)
                ys = myFov.latFull[bmnum]
                cs = np.ones((len(xs), len(ys))) * np.nan

#                fov_dtm = df.datetime.unique().tolist()
#                rsep = df.rsep.unique().tolist()
#                # check whether site parameters has changed within the interval of interest
#                if len(fov_dtm) <= 1:
#                    fov_dtm = fov_dtm[0]
#                    rsep = rsep[0]
#                    site = pydarn.radar.network().getRadarByCode(rad) \
#                           .getSiteByDate(fov_dtm)
#                    myFov = pydarn.radar.radFov.fov(site=site, ngates=rmax,
#                                                    nbeams=site.maxbeam,
#                                                    rsep=rsep, coords="mag",
#                                                    date_time=fov_dtm)
#                    ys = myFov.latFull[bmnum]
#                    cs = np.ones((len(xs), len(ys))) * np.nan
#
#                else:
#                    print("WARNING: more than one site/fov exist")
#                    print("Setting scatter_plot to True")
#                    scatter_plot = True
#                    xs = []
#                    ys = []
#                    cs = []

	    tcnt = 0
            for i, rw in df.iterrows():
                vl = json.loads(rw.vel)
                lat = json.loads(rw.mag_latc)
                slst = json.loads(rw.slist)
		dtm_tmp = pd.to_datetime(rw.datetime)
		relative_time = (dtm_tmp-event_dtm).total_seconds()/60.
		if scatter_plot:
		    # Select data between the mag_latc_range
		    vels_tmp = np.array([vl[i] for i in range(len(vl))\
			    if (lat[i] >= mag_latc_range[0] and lat[i] <= mag_latc_range[1])])
		    lats_tmp = np.array([lat[i] for i in range(len(lat))\
			    if (lat[i] >= mag_latc_range[0] and lat[i] <= mag_latc_range[1])])
		    xs.extend([relative_time]*len(lats_tmp))
		    ys.extend(lats_tmp)
		    cs.extend(vels_tmp)

		else:
		    # Build a list of datetimes to plot each data point at.
		    xs[tcnt] = relative_time
#		    if(i < len(df) - 1): 
#                        dtm_tmp_next = pd.to_datetime(df.iloc[i+1, :].datetime)
#                        relative_time_next = (dtm_tmp_next-event_dtm).total_seconds()/60.
#                        if(relative_time_next - xs[tcnt] > 4.):
#                            tcnt += 1
#		            # hardcoded 1 minute step per data point
#		            # but only if time between data points is > 4 minutes
#		            xs[tcnt] = xs[tcnt - 1] + 1.
#                    for j in range(len(slst)):
#                        cs[tcnt][slst[j]] = vl[j]
#		    tcnt += 1

                    for j in range(len(slst)):
                        cs[tcnt][slst[j]] = vl[j]
		    if(i < len(df) - 1): 
                        dtm_tmp_next = pd.to_datetime(df.iloc[i+1, :].datetime)
                        relative_time_next = (dtm_tmp_next-event_dtm).total_seconds()/60.
                        if(relative_time_next - xs[tcnt] > 4.):
                            tcnt += 1
		            # hardcoded 1 minute step per data point
		            # but only if time between data points is > 4 minutes
		            xs[tcnt] = xs[tcnt - 1] + 1.
		    tcnt += 1


            # Plot the data
            if scatter_plot:
                ccoll = ax.scatter(xs, ys, s=4.0, zorder=1,
                                   marker="s", c=cs,
                                   linewidths=.5, edgecolors='face',
                                   cmap=cmap, norm=norm)
            else:
                # Remove np.nan in ys
                if np.sum(np.isnan(ys)) > 0:
                    idx = np.where(np.isnan(ys))[0][0]
                    X, Y = np.meshgrid(xs[:tcnt], ys[:idx])
                    Z = np.ma.masked_where(np.isnan(cs[:tcnt, :idx].T), cs[:tcnt, :idx].T)
                else:
                    X, Y = np.meshgrid(xs[:tcnt], ys)
                    Z = np.ma.masked_where(np.isnan(cs[:tcnt, :].T), cs[:tcnt, :].T)
                ccoll = ax.pcolormesh(X, Y, Z, edgecolor=None, cmap=cmap, norm=norm)

            # Annotate the starting MLT location of the radar
            rad_mlt_loc = round(df.rad_mlt.as_matrix()[0]/15. + stable_interval/60.,1) % 24
            #lbl = rad + ",b" + str(bmnum) + "\nMLT=" + str(rad_mlt_loc) + "\nlag=" + str(lag_time) + "min"
            lbl = "Beam " + str(bmnum)
            ax.annotate(lbl, xy=(0.85, 0.82), xycoords="axes fraction", fontsize=8)
    ax.set_ylabel("MLAT", fontsize=9)
    ax.set_ylim([mag_latc_range[0], mag_latc_range[1]])
    ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)

    # Close conn
    conn.close()

    return ccoll


if __name__ == "__main__":

    from matplotlib.colors import BoundaryNorm, Normalize

    mag_latc_range=[53, 65]
    imf_ylim = [-7, 7]
    scatter_plot = False
    cmap="jet_r"
    norm = Normalize(vmin=-100,vmax=100)

    stable_interval=30
    IMF_turning = "southward"
    #IMF_turning = "northward"
    event_status = "good"
    event_rads = "all"
    #event_rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw"]
    geomag_condition="quiet"
    df_turn = build_event_database(IMF_turning=IMF_turning, event_status=event_status,
                                   geomag_condition=geomag_condition, rads=event_rads)

    # Run for all events
#    event_dtms = [pd.to_datetime(x) for x in df_turn.datetime.unique()]
    event_dtms = [dt.datetime(2011, 11, 8, 11, 23)]    # southward turning
    #event_dtms = [dt.datetime(2012, 1, 9, 10, 25)]      # northward turning
	
#    rads_list = [["adw"], ["ade"], ["cvw"], ["cve"], ["fhw"], ["fhe"], ["bks"], ["wal"]]
#    bmnums_lst = [[range(9, 21, 1)], [range(0, 12, 1)],  [range(9, 21, 1)], [range(0, 12, 1)],
#                  [range(9, 21, 1)], [range(0, 12, 1)],  [range(9, 21, 1)], [range(0, 12, 1)]]

    rads_list = [["cvw"]]
    bmnums_lst = [[range(9, 21, 1)]]
    #bmnums_lst = [[range(1, 20, 2)]]
    #bmnums_lst = [[range(0, 12, 1)]]
    #lag_time=10
    #lag_time=None

    #fig_dir = "/home/muhammad/Dropbox/tmp/tmp/"
    fig_dir = "../plots/rti_stackplot/" + IMF_turning + "/quiet_time/"
    #fig_dir = "../plots/rti_stackplot/single_event_per_fig/"

    for event_dtm in event_dtms:

        # Find the convection respond lag time
        df_tmp = build_event_database(IMF_turning=IMF_turning, event_status=event_status,
                                      geomag_condition=geomag_condition, rads=event_rads)
        df_tmp = df_tmp.loc[df_tmp.datetime==event_dtm, :]
        lag_time = df_tmp.lag_time.iloc[0]

        for b, rads in enumerate(rads_list):
            bmnums_list = bmnums_lst[b]
            nrows = np.sum([len(x) for x in bmnums_list])
            #nrows = 5
            fig, axes = plt.subplots(nrows=nrows+1, ncols=1, figsize=(6,15),
                                     sharex=True)
            ax_idx = 1

            # Plot LOS velocity 
            mappable = None
            for k, rad in enumerate(rads):
                bmnums = bmnums_list[k]
                for i, bmnum in enumerate(bmnums):
                    #ax = axes[(len(bmnums))*k+i]
                    ax = axes[ax_idx]
                    losvel_mappable = plot_rti(ax, event_dtm, rad,
                                               bmnum=bmnum, lag_time=lag_time, mag_bmazm=None,
                                               stable_interval=stable_interval,
                                               ftype="fitacf", mag_latc_range=mag_latc_range,
                                               cmap=cmap, norm=norm, scatter_plot=scatter_plot,
                                               IMF_turning=IMF_turning, db_name = None,
                                               dbdir = "../data/sqlite3/")
                    ax_idx = ax_idx + 1
                    if losvel_mappable:
                        mappable = losvel_mappable

            stm_title = event_dtm + dt.timedelta(seconds=60. * (lag_time - 30))
            etm_title = event_dtm + dt.timedelta(seconds=60. * (lag_time + 30))
            axes[0].set_title(stm_title.strftime("%m/%d/%Y    ") +\
                              stm_title.strftime("%H:%M - ") +\
                              etm_title.strftime("%H:%M UT") +\
                              "    " + rads[0].upper())
            axes[-1].set_xlabel("Time [min]")

            ##############################
            from superpose_single_event_losvel import plot_aualae
            ax_imf = axes[0]
            ax_theta = None
            # Plot IMF
            plot_imf(event_dtm, ax_imf=ax_imf, ax_theta=ax_theta,
                     stable_interval=stable_interval, ylim=imf_ylim,
                     dbdir="../data/sqlite3/", db_name="gmi_imf.sqlite",
                     table_name="IMF")
	    ax_imf.set_ylabel("IMF [nT]", fontsize=9)

#            # Plot AU, AL
#            plot_aualae(event_dtm, ax_ae, plot_ae=True, lag_time=lag_time,
#                        stable_interval=stable_interval, ylim_au=[0, 600],
#                        ylim_al=[-500, 0], ylabel_fontsize=9,
#                        marker='.', linestyle='--')
            ##############################


            # Add colorbar for LOS Vel.
            if mappable:
                fig.subplots_adjust(right=0.8)
                cbar_ax = fig.add_axes([0.85, 0.30, 0.03, 0.4])
                #fig.colorbar(im, cax=cbar_ax)
                add_cbar(fig, mappable, label="Velocity [m/s]", cax=cbar_ax,
                         ax=None, title_size=10, ytick_label_size=10)

            if scatter_plot:
                fig_name = "imf_scatter_losvel_" + event_dtm.strftime("%Y%m%d.%H%M_") + IMF_turning + "_" + "_".join(rads)
            else:
                fig_name = "imf_losvel_" + event_dtm.strftime("%Y%m%d.%H%M_") + IMF_turning + "_" + "_".join(rads)
            fig.savefig(fig_dir + fig_name + ".png", dpi=200, bbox_inches="tight")
            plt.close(fig)


