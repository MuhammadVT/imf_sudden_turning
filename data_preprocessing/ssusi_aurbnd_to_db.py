import sqlite3
import pandas as pd
import numpy as np
import datetime as dt
import json
from davitpy.utils.coordUtils import coord_conv

def read_ssusi_aurora_data(cdate, file_dir,
                           sat_num="f16", hemi="north"):
    """Reads DMSP SSUSI EDR-AUR data type"""
    import netCDF4
    import glob

    dtms = []
    mlat = []
    mlt = []
    fl_dir = file_dir + sat_num +  "/" + cdate.strftime("%Y%m%d") + "/"
    fnames = glob.glob(fl_dir + "*EDR-AURORA*")
    for file_name in fnames:
        # Convert data format from netCDF4 to pandas DataFrame
        ds = netCDF4.Dataset(file_name)
        magnetic_latitude = ds.variables[hemi.upper() + "_GEOMAGNETIC_LATITUDE"][:]
        magnetic_local_time = ds.variables[hemi.upper() + "_MAGNETIC_LOCAL_TIME"][:]
        ut_seconds =  ds.variables["TIME"][:]
        ut_time = cdate + dt.timedelta(seconds=ut_seconds + 0)
        #mlat.append("_".join([str(x) for x in magnetic_latitude]))
        #mlt.append("_".join([str(x) for x in magnetic_local_time]))
        mlat.append(json.dumps([round(x, 2) for x in magnetic_latitude]))
        mlt.append(json.dumps([round(x, 2) for x in magnetic_local_time]))
        dtms.append(ut_time)

    # Construct a dataframe
    # Drop seconds and milleseconds
    dtms = [x.replace(microsecond=0) for x in dtms]
    df = pd.DataFrame(data={"mlat":mlat, "mlt":mlt, "hemi":hemi,
                            "sat_num":sat_num, "datetime":dtms})

    # Sort by datetime
    df = df.sort_values("datetime")

    return df


def move_to_db(df, table_name=None,
               db_name=None, dbdir="../data/sqlite3/"):

    """Writes DMSP SSUSI data to a sqlite db """

    if db_name is None:
        db_name = "ssusi_aur_bnd.sqlite"
    if table_name is None:
        table_name = "aur_bnd"

    # Make a db connection
    conn = sqlite3.connect(dbdir + db_name)

    schema = "CREATE TABLE IF NOT EXISTS {tb} (" +\
             "datetime TIMESTAMP, "+\
             "hemi TEXT, mlat TEXT, mlt TEXT, " +\
             "sat_num TEXT, " +\
             "PRIMARY KEY(datetime, sat_num))"
    schema = schema.format(tb=table_name)

    # Write data to db
    print("Writing data to db")
    df.to_sql(table_name, conn, schema=schema, if_exists="append", index=False)
    print("Done!")

    # Close db connection
    conn.close()

    return


if __name__ == "__main__":

    import sys
    sys.path.append("../data/")
    from create_event_list import create_event_list

    # Create event list
    IMF_turning = "all"
    IMF_events = True
    df_turn = create_event_list(IMF_events=IMF_events, IMF_turning=IMF_turning)
    
    # Find the convection respond time
    turn_dtms = [x.to_pydatetime() for x in df_turn.datetime]
    event_dates = [dt.datetime(x.year, x.month, x.day) for x in turn_dtms]

    event_dates = [event_dates[0]]

    dbdir = "../data/sqlite3/"
    db_name = "ssusi_aur_bnd.sqlite"
    table_name = "aur_bnd"
    file_dir = "../data/ssusi/"

    #sat_nums = ["f16", "f17", "f18", "f19"]
    sat_nums = ["f17"]
    hemi="north"

    for cdate in event_dates:
        for sat_num in sat_nums:
            # Read the original data into a DF
            df = read_ssusi_aurora_data(cdate, file_dir=file_dir,
                                        sat_num=sat_num, hemi=hemi)

#            # Move data to DB
#            move_to_db(df, table_name=table_name,
#                       db_name=db_name, dbdir=dbdir)

