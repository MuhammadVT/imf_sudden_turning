import sys
sys.path.append("../../data/")
from build_event_database import build_event_database
import pandas as pd
import numpy as np
import datetime as dt


"""Converts bnd-fitcoeffs to bnd locations and write the results into files"""
def convert_fitcoeff_to_loc(df):

    """
    Calculates Latitude value of the Equatorward Auroral Boundary
    """
    dat = {"MLAT":[], "MLON":[], "date":[], "time":[]}
    mlon = np.arange(0.0, 360., 15.)
    mlon = np.append(mlon, 0.0)
    for i, item in df.iterrows():
        p_0 = np.array([item.p_0]*len(mlon))
        p_1 = np.array([item.p_1]*len(mlon))
        p_2 = np.array([item.p_2]*len(mlon))
        mlat = np.round(p_0 + p_1 * np.cos(2*np.pi*(mlon/360.)+p_2), 1)

        dat["MLAT"].extend(mlat)
        dat["MLON"].extend(mlon)
        dat["date"].extend([item.datetime.strftime("%Y%m%d")]*len(mlon))
        dat["time"].extend([item.datetime.strftime("%H%M")]*len(mlon))

    # Construct a DataFrame
    df_loc = pd.DataFrame(data=dat)

    return df_loc

IMF_turning = "southward"
#IMF_turning = "northward"
event_status = "good"
#rads = "all"
rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw"]
geomag_condition="quiet"
stable_interval=30

df_turn = build_event_database(IMF_turning=IMF_turning, event_status=event_status,
                               geomag_condition=geomag_condition, rads=rads)

imf_dtms = [x for x in pd.to_datetime(df_turn.datetime.unique())]
#imf_dtms = [imf_dtms[0]]

# Plot Auroral boundary for single events
for imf_dtm in imf_dtms:

    if imf_dtm.year <= 2012:
        if imf_dtm.strftime("%Y%m%d") == "20120229":
            continue
        # Read fitcoeff from file
        #bndcoeff_fdir = "../../data/poes/bnd_fitcoeff/iqr_05/"
        bndcoeff_fdir = "/sd-data/backup/muhammad/muhammad_data/poes/bnd_fitcoeff/"
        fitcoeff_file = bndcoeff_fdir + "poes-fit-" + imf_dtm.strftime("%Y%m%d") + ".txt"
        colTypeDict = { "p_0" : np.float64, "p_1" : np.float64,
                        "p_2" : np.float64, "sat":np.str,
                        "date":np.str, "time":np.str }
        date_parser = lambda date, time: dt.datetime.strptime(date+time, "%Y%m%d%H%M")
        df = pd.read_csv(fitcoeff_file, delim_whitespace=True,
                         parse_dates={"datetime":[4, 5]},
                         date_parser=date_parser,
                         dtype=colTypeDict)
        df_loc = convert_fitcoeff_to_loc(df)

        # Write to an output file
        #bnd_fdir = "../../data/poes/bnd/iqr_05/"
        bnd_fdir = "../../data/poes/bnd/"
        bnd_file = bnd_fdir + "poes-fit-" + imf_dtm.strftime("%Y%m%d") + ".txt"
        df_loc.to_csv(bnd_file, header=True, index=False, sep=" ")



