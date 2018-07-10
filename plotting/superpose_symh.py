import sys
sys.path.append("../data/")
from build_event_database import build_event_database
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import matplotlib.pyplot as plt
plt.style.use("ggplot")

def plot_superposed_symh(ax, df_turn=None, stable_interval=30, 
                        IMF_turning="northward"):

    """ Superimposes SymH for IMF turnings

    Parameters
    ----------
    stable_interval : minutes
        The interval length before or after the turning

    """

    if df_turn is None:
        df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")
    #df_turn = df_turn.tail()
    dbdir = "../data/sqlite3/"
    table_name = "symh"
    db_name = "gmi_imf.sqlite"

    # make a connection
    conn = sqlite3.connect(dbdir + db_name,
                           detect_types = sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    dtms = pd.to_datetime(df_turn.datetime.unique())
    for dtm in dtms:
        stm = dtm - dt.timedelta(seconds=60. * stable_interval)
        etm = dtm + dt.timedelta(seconds=60. * stable_interval)
        # load data to a dataframe
        command = "SELECT symh, datetime FROM {tb} " + \
                  "WHERE datetime BETWEEN '{stm}' AND '{etm}' "
        command = command.format(tb=table_name, stm=stm, etm=etm)
        df = pd.read_sql(command, conn)
        dtms_tmp = pd.to_datetime(df.datetime)
        df.loc[:, "relative_time"] = [(x-dtm).total_seconds()/60. for x in dtms_tmp]
        ax.plot(df.relative_time, df.loc[:, "symh"])
        ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)

    #ax.legend()

    return

if __name__ == "__main__":


    fig, ax = plt.subplots()
    IMF_turning = "southward"
    #IMF_turning = "northward"
    event_status = "good"
    stable_interval=30
    df_events = build_event_database(IMF_turning=IMF_turning, event_status=event_status)
    nevents = len(df_events.datetime.unique())

    plot_superposed_symh(ax, df_turn=df_events,
                         stable_interval=stable_interval, IMF_turning=IMF_turning)
    ax.set_ylim([-80, 30])
    ax.set_xlabel("Time [min]")
    ax.set_ylabel("SymH [nT]")
    ax.set_title(str(nevents) + " " + IMF_turning.capitalize() + " IMF Turnings")

    #fig_dir = "/home/muhammad/Dropbox/tmp/tmp/"
    fig_dir = "../plots/superposed_symh/"
    fig_name = "symh_for_" + str(nevents) + "_" + IMF_turning + "_turnings_IMF"

    fig.savefig(fig_dir + fig_name + ".png", bbox_inches="tight")
    #plt.show()



