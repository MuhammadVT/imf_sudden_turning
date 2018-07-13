import datetime as dt
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.style.use("ggplot")
import aacgmv2
import sys
sys.path.append("../data/")
from build_event_database import build_event_database

def mlat_vs_mlt(axes, df_turn=None, IMF_turning="northward",
	        raw_fdir="../data/poes/raw/", bnd_fdir="../data/poes/raw/",
	        coords = "mlt", read_bnd_from_file=True, file_source="bharat"):

    """Superposes POES determined auroral boundary"""

    from poes import get_aur_bnd
    if df_turn is None:
        df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")

    imf_dtms = pd.to_datetime(df_turn.datetime.unique())
    del_time = 30    # approximate relative time from the flow response time. Used to get the real relative time
    turning_txt = ["Before Turning", "After Turning"]
    for imf_dtm in imf_dtms:
	print(imf_dtm)
	# Get the lag time in flow response
	tmp_var = df_turn.loc[df_turn.datetime==imf_dtm, :]
	lag_time = tmp_var.lag_time.iloc[0]

        for i, reltime_sign in enumerate([-1, 1]):
	    ax = axes[i]

	    # Find an appropriate datetime for POES
	    sd_dtm = imf_dtm + dt.timedelta(seconds=60. * lag_time)
	    if reltime_sign == -1:
		reltime = (-1) * (sd_dtm.minute % del_time)
	    elif reltime_sign == 1:
		reltime = del_time - (sd_dtm.minute % del_time)
	    poes_dtm = sd_dtm + dt.timedelta(seconds=60. * reltime)
	    selTime = poes_dtm

	    # two ways to overlay estimated boundary!
	    # Fast way
	    if read_bnd_from_file:
		if file_source == "bharat":
		    inpFileName = bnd_fdir + "poes-fit-" + selTime.strftime("%Y%m%d") + ".txt"
		try:
		    print "reading boundary data from-->", inpFileName
		    colTypeDict = { "MLAT" : np.float64, "MLON" : np.float64,\
				     "date":np.str, "time":np.str }
		    estBndDF = pd.read_csv(inpFileName, delim_whitespace=True,\
						     dtype=colTypeDict)
		    # get the selected time
		    estBndDF = estBndDF[ ( estBndDF["date"] == selTime.strftime("%Y%m%d") ) &\
					( estBndDF["time"] == selTime.strftime("%H%M") )\
					 ].reset_index(drop=True)
                except IOError:
                    print(inpFileName + " does not exist.")
                    pass
	    # Slow way
	    else:
		poesRdObj = get_aur_bnd.PoesAur()
		( poesAllEleDataDF, poesAllProDataDF ) = poesRdObj.read_poes_data_files(\
							    poesRawDate=selTime,\
							    poesRawDir=raw_fdir )
		aurPassDF = poesRdObj.get_closest_sat_passes( poesAllEleDataDF,\
				    poesAllProDataDF, [selTime, selTime] )
		# determine auroral boundaries from all the POES satellites
		# at a given time.
		eqBndLocsDF = poesRdObj.get_nth_ele_eq_bnd_locs( aurPassDF,\
								poesAllEleDataDF )
		estBndDF = poesRdObj.fit_circle_aurbnd(eqBndLocsDF, save_to_file=False)

	    # convert to MLT coords if needed
	    if not estBndDF.empty:
		if coords == "mlt":
		    estBndDF["selTime"] = selTime
		    estBndDF["MLT"] = estBndDF.apply( get_mlt, axis=1 )
		else:
		    pass
	    else:
		"No Data"

	    # Plot fitted Aur. Bnd. 
	    if not estBndDF.empty:
		xs = [x if x<12 else x-24 for x in estBndDF.MLT.tolist()]
		ys = estBndDF.MLAT
		xs = xs[:-1]
		ys = ys[:-1]
		ax.plot(xs, ys, marker="o", markersize=2, linewidth=1.5)

    for j, ax in enumerate(axes):
	# Set x-lim
	ax.set_xlim([-6, 6])
	ax.set_ylim([50, 75])
	ax.axhline(y=58, color="k", linestyle="--", linewidth=1.0)

	# Set titles, labels
	ax.set_title(turning_txt[j])
	ax.set_xlabel("MLT")
    axes[0].set_ylabel("MLAT")

	
		
    return

def get_mlt(row):
    # given the est bnd df, time get MLT from MLON
    return np.round( aacgmv2.convert_mlt( row["MLON"],
			 row["selTime"] ), 1 )


if __name__ == "__main__":

    raw_fdir="../data/poes/raw/"
    bnd_fdir="../data/poes/bnd/"
    #IMF_turning="northward"
    IMF_turning="southward"
    read_bnd_from_file=True
    coords = "mlt"

    df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")
    #df_turn = df_turn.head(30)
    nevents = len(df_turn.datetime.unique())

    # Superpose Auroral Boundaries in MLAT vs MLT format
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 5), sharey=True)
    mlat_vs_mlt(axes, df_turn=df_turn, IMF_turning=IMF_turning,
	        raw_fdir=raw_fdir, bnd_fdir=bnd_fdir,
	        coords=coords, read_bnd_from_file=read_bnd_from_file,
		file_source="bharat")
    # Set a figure title
    fig_title = str(nevents) + " IMF " + IMF_turning.capitalize() + " Turning Events"
    fig.suptitle(fig_title, y=1.00, fontsize=15)

    #fig_dir = "/home/muhammad/Dropbox/tmp/tmp/"
    fig_dir = "../plots/poes_aur_bnd/"
    fig_name = "superposed_poes_aur_bnd_" + str(nevents) + "_" + IMF_turning + "_turnings_IMF"
    fig.savefig(fig_dir + fig_name + ".png", bbox_inches="tight")

    #plt.show()	
    plt.close(fig)

