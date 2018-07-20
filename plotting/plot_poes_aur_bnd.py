import datetime as dt
import pandas as pd
import numpy as np
from poes import poes_plot_utils
from davitpy import utils
import matplotlib.pyplot as plt
import sys
sys.path.append("../data/")
from build_event_database import build_event_database

def plot_poes_aur_bnd(selTime, ax, satList, raw_fdir="../data/poes/raw/",
		      bnd_fdir="../data/poes/bnd/", read_bnd_from_file=True,
                      remove_outliers=True, cutoff_iqr_prop=1.5,
		      coords = "mlt", plotCBar=True, cax=None,
                      cbar_shrink=1.0, titleString=None):	

    """Plot POES determined auroral boundary for a single event"""

    # Plot all closest (in time) satellite passes at a given time
    # and also overlay the estimated auroral boundary
    pltDate = dt.datetime(selTime.year, selTime.month, selTime.day)

    m = utils.plotUtils.mapObj(ax=ax, width=80*111e3, height=80*111e3, coords=coords,\
			       lat_0=90., lon_0=0, datetime=selTime)
    poesPltObj = poes_plot_utils.PlotUtils(pltDate, pltCoords=coords)
    poesPltObj.overlay_closest_sat_pass(selTime, m, ax, raw_fdir, satList=satList, 
                                        plotCBar=plotCBar, cax=cax, timeFontSize=4., timeMarkerSize=2.,
                                        overlayTimeInterval=1, timeTextColor="red",
                                        cbar_shrink=cbar_shrink)

    # two ways to overlay estimated boundary!
    # poesPltObj.overlay_equ_bnd(selTime,m,ax,rawSatDir="/tmp/poes/raw/")
    if read_bnd_from_file:
	inpFileName = bnd_fdir + "poes-fit-" + selTime.strftime("%Y%m%d") + ".txt"
        try:
            poesPltObj.overlay_equ_bnd(selTime, m, ax, inpFileName=inpFileName,
                                       linewidth=1, linecolor="red", line_zorder=7)
        except IOError:
            print(inpFileName + " does not exist.")
            pass
    else:
	poesPltObj.overlay_equ_bnd(selTime, m, ax, raw_fdir,
                                   remove_outliers=remove_outliers, cutoff_iqr_prop=cutoff_iqr_prop,
				   linewidth=1, linecolor="red", line_zorder=7)

    return

#satList_dict = {dt.datetime(2013, 2, 21, 5, 34):{"reltime":6, "satList":["m01", "m02", "n15", "n16", "n17", "n18", "n19"], "IMF_turning":"northward"},
#                dt.datetime(2013, 2, 21, 5, 34):{"reltime":6, "satList":["m01", "m02", "n15", "n16", "n17", "n18", "n19"], "IMF_turning":"northward"}}

satList = ["m01", "m02", "n15", "n16", "n17", "n18", "n19"]
#satList = ["m01", "n15", "n18", "n19"]
if __name__ == "__main__":

    raw_fdir="../data/poes/raw/"
    #bnd_fdir="../data/poes/bnd/"
    bnd_fdir="../data/poes/bnd_tmp/"
    read_bnd_from_file=True
    remove_outliers=True
    cutoff_iqr_prop=1.5
    coords = "mlt"
    reltime_signs = [-1, 1]   # -1 indicates time before turning
    del_time = 30    # approximate relative time from the flow response time. Used to get the real relative time
    IMF_turning="northward"
    cbar_shrink = 1.0

    # Plot Auroral boundary for single events
    df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")
    imf_dtms = [x for x in pd.to_datetime(df_turn.datetime.unique())]
    #imf_dtms = [imf_dtms[0]] 
    reltimes = []
    for imf_dtm in imf_dtms:
        print("Plotting POES Auroral Boundary for " + str(imf_dtm))
        # Create a fig
        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12,6))
	fig.subplots_adjust(right=0.82)
	cbar_ax = fig.add_axes([0.85, 0.2, 0.02, 0.60])

        plotCBar_flags = [False, True]

        # Get the lag time in flow response
        tmp_var = df_turn.loc[df_turn.datetime==imf_dtm, :]
        lag_time = tmp_var.lag_time.iloc[0]

	for i, reltime_sign in enumerate(reltime_signs):
	    ax = axes[i]
	    # Find an appropriate datetime for POES
	    sd_dtm = imf_dtm + dt.timedelta(seconds=60. * lag_time)
	    if reltime_sign == -1:
#		if (sd_dtm.minute % del_time) < 5:
#		    reltime = (-1) * (del_time + sd_dtm.minute % del_time)
#		else:
#		    reltime = (-1) * (del_time - sd_dtm.minute % del_time)
		if (sd_dtm.minute % del_time) == 0:
                    reltime = (-1) * del_time
                else:
                    reltime = (-1) * (sd_dtm.minute % del_time)
	    elif reltime_sign == 1:
#		if (sd_dtm.minute % del_time) < 5:
#		    reltime = del_time - sd_dtm.minute % del_time 
#		else:
#		    reltime = del_time + (del_time - sd_dtm.minute % del_time)
                reltime = del_time - (sd_dtm.minute % del_time)
            reltimes.append(reltime)
	    poes_dtm = sd_dtm + dt.timedelta(seconds=60. * reltime)

	    # Make a title
	    title_str = poes_dtm.strftime("%Y-%m-%d  %H:%M") + " UT"
	    
    	    # Plot Auroral Boundary
            #satList = satList_dict[imf_dtm]["satList"]
	    plot_poes_aur_bnd(poes_dtm, ax, satList, read_bnd_from_file=read_bnd_from_file, titleString=title_str,
			      raw_fdir=raw_fdir, bnd_fdir=bnd_fdir, plotCBar=plotCBar_flags[i], cax=cbar_ax,
                              remove_outliers=remove_outliers, cutoff_iqr_prop=cutoff_iqr_prop,
			      coords=coords , cbar_shrink=cbar_shrink)

        # Put a figure title
        fig_title = "IMF " + IMF_turning.capitalize() + " Turning " + sd_dtm.strftime("%Y-%m-%d  %H:%M") + " UT"
        fig.suptitle(fig_title, y=0.93, fontsize=15)

        # Save figure
        fig_dir = "../plots/poes_aur_bnd/tmp/"
        #fig_dtm_txt = imf_dtm.strftime("%Y%m%d.%H%M") + "_reltime_" + str(reltime)
        #fig_name = "poes_aur_bnd_" + IMF_turning + "_IMF_turning_" + fig_dtm_txt + ".png"
        fig_dtm_txt = imf_dtm.strftime("%Y%m%d.%H%M")
        fig_name = "1_by_2_poes_aur_bnd_" + IMF_turning + "_IMF_turning_" + fig_dtm_txt + ".png"
        fig.savefig(fig_dir + fig_name, dpi=200, bbox_inches='tight')
        plt.close(fig)

