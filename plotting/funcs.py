import sqlite3
import pandas as pd
import numpy as np


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

    return



if __name__ == "__main__":

    import datetime as dt
    
    rad = "cve"
    stm = dt.datetime(2013, 11, 16, 6, 55)
    etm = dt.datetime(2013, 11, 16, 7, 55)
    bmnum = find_bmnum(rad, stm, etm, mag_bmazm=-30,
                       bmazm_diff=3.,
                       db_name="sd_gridded_los_data_fitacf.sqlite",
                       dbdir = "../data/sqlite3/")

