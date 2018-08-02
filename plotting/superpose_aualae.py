import sys
sys.path.append("../data/")
from build_event_database import build_event_database
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3
import matplotlib.pyplot as plt
plt.style.use("ggplot")

def plot_superposed_aualae(ax, param="au", df_turn=None, stable_interval=30, 
                           ylim_au=[0, 500], ylim_al=[-500, 0], 
                           IMF_turning="northward"):

    """ Superimposes AU, AL, AE for IMF turnings

    Parameters
    ----------
    stable_interval : minutes
        The interval length before or after the turning

    """

    if df_turn is None:
        df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")

    #df_turn = df_turn.tail()
    dbdir = "../data/sqlite3/"
    table_name = "aualae"
    db_name = "gmi_imf.sqlite"

    # make a connection
    conn = sqlite3.connect(dbdir + db_name,
                           detect_types = sqlite3.PARSE_DECLTYPES)
    cur = conn.cursor()

    color_dict = {"AU":"k","AL":"g", "AE":"r"}

    dtms = pd.to_datetime(df_turn.datetime.unique())
    for dtm in dtms:
        # Add the lag time
        tmp_var = df_turn.loc[df_turn.datetime==dtm, :]
        lag_time = tmp_var.lag_time.iloc[0]
        dtm = dtm + dt.timedelta(seconds=60. * lag_time)

        # load data to a dataframe
        stm = dtm - dt.timedelta(seconds=60. * stable_interval)
        etm = dtm + dt.timedelta(seconds=60. * stable_interval)
        command = "SELECT au, al, ae, datetime FROM {tb} " + \
                  "WHERE datetime BETWEEN '{stm}' AND '{etm}' "
        command = command.format(tb=table_name, stm=stm, etm=etm)
        df = pd.read_sql(command, conn)
        dtms_tmp = pd.to_datetime(df.datetime)
        df.loc[:, "relative_time"] = [(x-dtm).total_seconds()/60. for x in dtms_tmp]
        ax.plot(df.relative_time, df.loc[:, param], color=color_dict[param.upper()])
    ax.axvline(x=0, color="r", linestyle="--", linewidth=1.)
    ax.set_ylim([ylim_al[0], ylim_au[1]])
    ax.locator_params(axis='y', nbins=6)

    # Add label
#    ax.annotate(param.upper(), xytext=(0.9, 0.9),
#                color=color_dict[param.upper()], textcoords="axes fraction")
    xy_dict = {"AU":(0.85, 0.7),"AL":(0.85, 0.25), "AE":(0.85, 0.8)}
    ax.annotate(param.upper(), xy=xy_dict[param.upper()], color=color_dict[param.upper()],
                xycoords="axes fraction", fontsize=15)

    return

if __name__ == "__main__":

    params = ["au", "al", "ae"]
    ylim_au=[-50, 500]; ylim_al=[-500, 50] 
    ylim_ae = [-50, 800]
    ylims = [ylim_au, ylim_al, ylim_ae]
    IMF_turning = "southward"
    #IMF_turning = "northward"
    event_status = "good"
    stable_interval=30
    df_events = build_event_database(IMF_turning=IMF_turning, event_status=event_status)
    nevents = len(df_events.datetime.unique())

    fig, axes = plt.subplots(nrows=len(params), ncols=1, sharex=True, figsize=(6,8))
    if len(params) == 1:
        axes = [axes]
    for i, param in enumerate(params):
        ax = axes[i]
        plot_superposed_aualae(ax, param=param, df_turn=df_events,
                            stable_interval=stable_interval, IMF_turning=IMF_turning)
        ax.set_ylim(ylims[i])
        ax.set_ylabel(param.upper() + " [nT]")
    axes[0].set_title(str(nevents) + " " + IMF_turning.capitalize() + " IMF Turnings")
    axes[-1].set_xlabel("Time [min]")

    fig_dir = "/home/muhammad/Dropbox/tmp/tmp/"
    #fig_dir = "../plots/superposed_aualae/"
    fig_name = "AUALAE_for_" + str(nevents) + "_" + IMF_turning + "_turnings_IMF" 
    fig.savefig(fig_dir + fig_name + ".png", bbox_inches="tight")
    plt.close(fig)
    #plt.show()



