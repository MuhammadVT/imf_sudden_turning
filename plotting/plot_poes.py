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
		      bnd_fdir="../data/poes/bnd/",
		      coords = "mlt", plotCBar=True, cbar_shrink=1.0):	

    """Plot POES determined auroral boundary for a single event"""

    # Plot all closest (in time) satellite passes at a given time
    # and also overlay the estimated auroral boundary
    pltDate = dt.datetime(selTime.year, selTime.month, selTime.day)

    m = utils.plotUtils.mapObj(width=80*111e3, height=80*111e3, coords=coords,\
			       lat_0=90., lon_0=0, datetime=selTime)
    poesPltObj = poes_plot_utils.PlotUtils(pltDate, pltCoords=coords)
    poesPltObj.overlay_closest_sat_pass(selTime, m, ax, raw_fdir, satList=satList, 
                                        plotCBar=plotCBar, timeFontSize=4., timeMarkerSize=2.,
                                        overlayTimeInterval=1, timeTextColor="red",
                                        cbar_shrink=cbar_shrink)

    # two ways to overlay estimated boundary!
    # poesPltObj.overlay_equ_bnd(selTime,m,ax,rawSatDir="/tmp/poes/raw/")
    if read_bnd_from_file:
	inpFileName = bnd_fdir + "poes-fit-" + selTime.strftime("%Y%m%d") + ".txt"
	poesPltObj.overlay_equ_bnd(selTime, m, ax, inpFileName=inpFileName,
				   linewidth=1, linecolor="red", line_zorder=7)
    else:
	poesPltObj.overlay_equ_bnd(selTime, m, ax, raw_fdir,
				   linewidth=1, linecolor="red", line_zorder=7)

    return


def plot_superposed_aur_bnd(ax, satList, df_turn=None, reltime=-20, IMF_turning="northward",
			    raw_fdir="../../data/poes/raw/", bnd_fdir="../../data/poes/raw/",
			    coords = "mlt", read_bnd_from_file=True, title_str=None):	

    """Superposes POES determined auroral boundary"""

    if df_turn is None:
        df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")

    imf_dtms = pd.to_datetime(df_turn.datetime.unique())
    for imf_dtm in imf_dtms:
	# Add the lag time in flow response
        tmp_var = df_turn.loc[df_turn.datetime==imf_dtm, :]
        lag_time = tmp_var.lag_time.iloc[0]
        dtm = imf_dtm + dt.timedelta(seconds=60. * lag_time)

	# Plot all closest (in time) satellite passes at a given time
	# and also overlay the estimated auroral boundary
	pltDate = dt.datetime(dtm.year, dtm.month, dtm.day)
	selTime = dtm + dt.timedelta(seconds = 60. * reltime)

	m = utils.plotUtils.mapObj(width=80*111e3, height=80*111e3, coords=coords,\
				   lat_0=90., lon_0=0, datetime=selTime)
	poesPltObj = poes_plot_utils.PlotUtils(pltDate, pltCoords=coords)
	poesPltObj.overlay_closest_sat_pass(selTime,m,ax, raw_fdir, titleString=title_str)

	# overlay estimated boundary!
	inpFileName = file_dir + "poes-fit-" + selTime.strftime("%Y%m%d") + ".txt"
	poesPltObj.overlay_equ_bnd(selTime, m, ax, inpFileName=inpFileName,
				   linewidth=1, linecolor="red", line_zorder=7)

    return

if __name__ == "__main__":

    raw_fdir="../data/poes/raw/"
    bnd_fdir="../data/poes/bnd/"
    satList = ["m01", "m02", "n15", "n16", "n17", "n18", "n19"]
    #satList = ["m01", "n15", "n18", "n19"]
    reltime_signs = [-1, 1]   # -1 indicates time before turning
    del_time = 10    # approximate relative time from the flow response time
    IMF_turning="northward"

    # Plot Auroral boundary for single events
    df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")
    imf_dtms = pd.to_datetime(df_turn.datetime.unique())
    imf_dtms = [imf_dtms[0]] 
    #imf_dtms = [dt.datetime(2015, 4, 9, 7, 30)]; lag_time = 0
    for imf_dtm in imf_dtms:
	for reltime_sign in reltime_signs:
	    fig = plt.figure(figsize=(12, 8))
	    ax = fig.add_subplot(1,1,1)

	    # Get the lag time in flow response
	    tmp_var = df_turn.loc[df_turn.datetime==imf_dtm, :]
	    lag_time = tmp_var.lag_time.iloc[0]

	    # Find an appropriate datetime for POES
	    poes_dtm = imf_dtm + dt.timedelta(seconds=60. * lag_time)
	    if reltime_sign == -1:
		if (poes_dtm.minute % 10) < 5:
		    reltime = (-1) * (del_time + poes_dtm.minute % 10)
		else:
		    reltime = (-1) * (10 - poes_dtm.minute % 10)
	    elif reltime_sign == 1:
		if (poes_dtm.minute % 10) < 5:
		    reltime = 10 - poes_dtm.minute % 10
		else:
		    reltime = del_time + (10 - poes_dtm.minute % 10)
	    poes_dtm = poes_dtm + dt.timedelta(seconds=60. * reltime)

	    # Make a title
	    title_str = poes_dtm.strftime("%Y-%m-%d  %H:%M") + " UT"
	    
    	    # Plot Auroral Boundary
	    plot_poes_aur_bnd(poes_dtm, ax, satList,
			      raw_fdir=raw_fdir, bnd_fdir=bnd_fdir,
			      coords = "mlt")

	    # Save figure
            fig_dir = "../plots/poes_aur_bnd/"
	    fig_dtm_txt = imf_dtm.strftime("%Y%m%d.%H%M") + "_reltime_" + str(reltime)
            fig_name = "poes_aur_bnd_" + IMF_turning + "_IMF_turning_" + fig_dtm_txt + ".png"
	    fig.savefig(fig_dir + fig_name, dpi=200, bbox_inches='tight')
	    plt.close(fig)

#    plot_superposed_imf(ax, satList, df_turn=None, reltime=-20, IMF_turning="northward",
#			raw_fdir="../../data/poes/raw/", bnd_fdir="../../data/poes/raw/",
#		 	coords = "mlt")	

