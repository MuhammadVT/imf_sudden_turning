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


if __name__ == "__main__":

    import datetime as dt
    
    rad = "cve"
    stm = dt.datetime(2013, 11, 16, 6, 55)
    etm = dt.datetime(2013, 11, 16, 7, 55)
    bmnum = find_bmnum(rad, stm, etm, mag_bmazm=-30,
                       bmazm_diff=3.,
                       db_name="sd_gridded_los_data_fitacf.sqlite",
                       dbdir = "../data/sqlite3/")



