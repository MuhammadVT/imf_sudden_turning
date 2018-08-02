import numpy as np
import datetime as dt
import logging
import sqlite3
import json
import sys
sys.path.append("../data/")
from build_event_database import build_event_database

def build_master_table(input_table, output_table, ftype="fitacf",
                       filtered_interval = 2.,
                       coords="mlt", dbdir="../data/sqlite3/",
                       input_dbname=None, output_dbname=None):
   
    """ combines all the median filtered gridded data into
    one master table. 
    The results are stored in a different db file.

    Parameters
    ----------
    input_table : str
        A table name from input_dbname
    output_table : str
        A table name from output_dbname
    ftype : str
        SuperDARN file type
    coords : str
        Coordinates in which the binning process took place.
        Default to "mlt, can be "geo" as well. 
    input_dbname : str, default to None
        Name of the sqlite db where xxx-min median data are stored.
    output_dbname : str, default to None
        Name of the master db

    Returns
    -------
    Nothing

    """

    # create db name
    if input_dbname is None:
        input_dbname = "sd_" + str(int(filtered_interval)) + "_min_median_" +\
                       coords + "_" + ftype + ".sqlite"
    if output_dbname is None:
        output_dbname = "sd_master_" + coords + "_" + ftype + ".sqlite"

    # make a connection to xxx-min median filtered data 
    try:
        conn_in = sqlite3.connect(dbdir + input_dbname,
                                  detect_types = sqlite3.PARSE_DECLTYPES)
        cur_in = conn_in.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    # make a connection to master db
    try:
        conn_out = sqlite3.connect(dbdir + output_dbname,
                                   detect_types = sqlite3.PARSE_DECLTYPES)
        cur_out = conn_out.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    # create a table
    if coords == "mlt":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel float(9,2)," +\
                  " mag_glatc float(7,2)," +\
                  " mag_gltc float(8,2)," +\
                  " mag_gazmc SMALLINT," +\
                  " datetime DATETIME, " +\
                  " rad VARCHAR(3), " +\
                  " CONSTRAINT all_rads PRIMARY KEY (" +\
                  "mag_glatc, mag_gltc, mag_gazmc, datetime, rad))"
    elif coords == "geo":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel float(9,2)," +\
                  " geo_glatc float(7,2)," +\
                  " geo_gltc float(8,2)," +\
                  " geo_gazmc SMALLINT," +\
                  " datetime DATETIME, " +\
                  " rad VARCHAR(3), " +\
                  " CONSTRAINT all_rads PRIMARY KEY (" +\
                  "geo_glatc, geo_gltc, geo_gazmc, datetime, rad))"
    command = command.format(tb=output_table)
    try:
        cur_out.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)

    if coords == "mlt":
	command = "SELECT vel, mag_glatc, mag_gltc, mag_gazmc, " +\
		  "datetime, rad FROM {tb1} ORDER By datetime ASC"
    elif coords == "geo":
	command = "SELECT vel, geo_glatc, geo_gltc, geo_gazmc, " +\
		  "datetime, rad FROM {tb1} ORDER By datetime ASC"
    command = command.format(tb1=input_table)

    # fetch the data
    try:
	cur_in.execute(command)
    except Exception, e:
	logging.error(e, exc_info=True)
    rows = cur_in.fetchall()

    # insert the data into a table
    if rows:
	if coords == "mlt":
	    command = "INSERT OR IGNORE INTO {tb2} (vel, mag_glatc, mag_gltc, " +\
		      "mag_gazmc, datetime, rad) VALUES (?, ?, ?, ?, ?, ?)"
	elif coords == "geo":
	    command = "INSERT OR IGNORE INTO {tb2} (vel, geo_glatc, geo_gltc, " +\
		      "geo_gazmc, datetime, rad) VALUES (?, ?, ?, ?, ?, ?)"
	command = command.format(tb2=output_table)
	for rw in rows:
	    vel, lat, lt, azm, dtm, rad = rw

	    # insert the data
	    try:
		cur_out.execute(command, (vel, lat, lt, azm, dtm, rad))
	    except Exception, e:
		logging.error(e, exc_info=True)

    # commit the change
    try:
	conn_out.commit()
    except Exception, e:
	logging.error(e, exc_info=True)

    # close db connections
    conn_in.close()
    conn_out.close()

    return

def build_superposed_master_table(output_table, df_events=None, half_interval_length=75,
                                  imf_lagtime=15, ftype="fitacf", coords="mlt",
                                  dbdir="../data/sqlite3/",
                                  input_dbname=None, output_dbname=None):
   
    """ combines all the gridded data (NOT median filtered) into
    one master table to do superposed epoch analysis. 
    The results are stored in a different db file.

    Parameters
    ----------
    output_table : str
        A table name from output_dbname
    ftype : str
        SuperDARN file type
    coords : str
        Coordinates in which the binning process took place.
        Default to "mlt, can be "geo" as well. 
    input_dbname : str, default to None
        Name of the sqlite db where xxx-min median data are stored.
    output_dbname : str, default to None
        Name of the master db

    Returns
    -------
    Nothing

    """

    # create db name
    if input_dbname is None:
        input_dbname = "sd_gridded_los_data_" + ftype + ".sqlite"
    if output_dbname is None:
        output_dbname = "sd_master_" + coords + "_" + ftype + ".sqlite"
    if df_events is None:
        df_events = build_event_database(IMF_turning="all", event_status="all")

    # make a db connection 
    try:
        conn_in = sqlite3.connect(dbdir + input_dbname,
                                  detect_types = sqlite3.PARSE_DECLTYPES)
        cur_in = conn_in.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    # make a connection to master db
    try:
        conn_out = sqlite3.connect(dbdir + output_dbname,
                                   detect_types = sqlite3.PARSE_DECLTYPES)
        cur_out = conn_out.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    # create a table
    if coords == "mlt":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel float(9,2)," +\
                  " mag_glatc float(7,2)," +\
                  " mag_gltc float(8,2)," +\
                  " mag_gazmc SMALLINT," +\
                  " mag_bmazm REAL," +\
                  " datetime DATETIME, " +\
                  " relative_time REAL, " +\
                  " rad VARCHAR(3), " +\
                  " CONSTRAINT all_rads PRIMARY KEY (" +\
                  "mag_glatc, mag_gltc, mag_gazmc, datetime, rad))"
    elif coords == "geo":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel float(9,2)," +\
                  " geo_glatc float(7,2)," +\
                  " geo_gltc float(8,2)," +\
                  " geo_gazmc SMALLINT," +\
                  " bmazm REAL," +\
                  " datetime DATETIME, " +\
                  " relative_time REAL, " +\
                  " rad VARCHAR(3), " +\
                  " CONSTRAINT all_rads PRIMARY KEY (" +\
                  "geo_glatc, geo_gltc, geo_gazmc, datetime, rad))"
    command = command.format(tb=output_table)
    try:
        cur_out.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)

    for i, df_row in df_events.iterrows():
        imf_dtm = df_row.datetime.to_pydatetime()
        response_dtm = imf_dtm + dt.timedelta(seconds=60. * df_row.lag_time)
        rad = df_row.rad

        # Build stm and etm
        lagged_imf_dtm = imf_dtm + dt.timedelta(seconds=60. * imf_lagtime)
        stm = lagged_imf_dtm - dt.timedelta(seconds=60. * half_interval_length)
        etm = lagged_imf_dtm + dt.timedelta(seconds=60. * half_interval_length)
        if coords == "mlt":
            command = "SELECT vel, mag_glatc, mag_gltc, mag_gazmc, mag_bmazm, " +\
                      "datetime FROM {tb1} "+\
                      "WHERE datetime BETWEEN '{stm}' AND '{etm}' "+\
                      "ORDER By datetime ASC"
        elif coords == "geo":
            command = "SELECT vel, geo_glatc, geo_gltc, geo_gazmc, bmazm, " +\
                      "datetime FROM {tb1} "+\
                      "WHERE datetime BETWEEN '{stm}' AND '{etm}' "+\
                      "ORDER By datetime ASC"
        command = command.format(tb1=rad, stm=stm, etm=etm)

        # fetch the data
        try:
            cur_in.execute(command)
        except Exception, e:
            logging.error(e, exc_info=True)
        rows = cur_in.fetchall()

        # insert the data into a table
        if rows:
            if coords == "mlt":
                command = "INSERT OR IGNORE INTO {tb2} (vel, mag_glatc, mag_gltc, " +\
                          "mag_gazmc, mag_bmazm, datetime, relative_time, rad) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            elif coords == "geo":
                command = "INSERT OR IGNORE INTO {tb2} (vel, geo_glatc, geo_gltc, " +\
                          "geo_gazmc, bmazm, datetime, relative_time, rad) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            command = command.format(tb2=output_table)
            for rw in rows:
                vels, lats, lts, azms, bmazm, dtm = rw
                if vels:
                    vels = json.loads(vels)
                    lats = json.loads(lats)
                    lts = json.loads(lts)
                    azms = json.loads(azms)
                    relative_time = int(round((dtm - response_dtm).total_seconds()/60.))
                    
                    for i in range(len(vels)):
                        vel = vels[i]
                        lat = lats[i]
                        lt = lts[i]
                        azm = azms[i]
                        # insert the data
                        try:
                            cur_out.execute(command, (vel, lat, lt, azm, bmazm, dtm, relative_time, rad))
                        except Exception, e:
                            logging.error(e, exc_info=True)
                else:
                    continue

            # commit the change
            try:
                conn_out.commit()
            except Exception, e:
                logging.error(e, exc_info=True)
        print("Done with event ", df_row)

    # close db connections
    conn_in.close()
    conn_out.close()

    return

def master_summary(input_table, output_table, coords="mlt",
                   db_name=None, dbdir="../data/sqlite3/"):
    
    """ stores the summay statistics of the data in master table into 
    a different table in the same database.
    Time and rad informatin are all lost at this point.

    Parameters
    ----------
    input_table : str
        Name of a master table in master db
    output_table : str
        Name of a master_summary table in master db
    coords : str
        Coordinates in which the binning process took place.
        Default to "mlt, can be "geo" as well. 
    config_filename: str
        name and path of the configuration file
    section: str, default to "midlat"
        section of database configuration
    db_name : str, default to None
        Name of the master db

    Returns
    -------
    Nothing

    """
    import datetime as dt
    import numpy as np 
    import sqlite3
    import logging

    # construct a db name
    if db_name is None:
        db_name = "sd_master_" + coords + "_" + ftype + ".sqlite"

    # make a connection to master table
    try:
        conn = sqlite3.connect(dbdir + db_name,
                               detect_types = sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    # create a table
    if coords == "mlt":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel_mean float(9,2)," +\
                  " vel_median float(9,2)," +\
                  " vel_std float(9,2)," +\
                  " vel_count INT," +\
                  " mag_glatc float(7,2)," +\
                  " mag_gltc float(8,2)," +\
                  " mag_gazmc SMALLINT," +\
                  " CONSTRAINT master_summary PRIMARY KEY (" +\
                  "mag_glatc, mag_gltc, mag_gazmc))"
    elif coords == "geo":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel_mean float(9,2)," +\
                  " vel_median float(9,2)," +\
                  " vel_std float(9,2)," +\
                  " vel_count INT," +\
                  " geo_glatc float(7,2)," +\
                  " geo_gltc float(8,2)," +\
                  " geo_gazmc SMALLINT," +\
                  " CONSTRAINT master_summary PRIMARY KEY (" +\
                  "geo_glatc, geo_gltc, geo_gazmc))"
    command = command.format(tb=output_table)
    try:
        cur.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)

    if coords == "mlt":
	command = "SELECT AVG(vel), STD(vel), COUNT(vel), " +\
                  "mag_glatc, mag_gltc, mag_gazmc " +\
		  "FROM {tb1} GROUP BY mag_glatc, mag_gltc, mag_gazmc"
    elif coords == "geo":
	command = "SELECT AVG(vel), STD(vel), COUNT(vel), " +\
                  "geo_glatc, geo_gltc, geo_gazmc " +\
		  "FROM {tb1} GROUP BY geo_glatc, geo_gltc, geo_gazmc"
    command = command.format(tb1=input_table)

    # fetch the data
    try:
	cur.execute(command)
    except Exception, e:
	logging.error(e, exc_info=True)
    rows = cur.fetchall()

    # insert the data into a table
    if rows:
	if coords == "mlt":
	    command = "INSERT OR IGNORE INTO {tb2} (vel_mean, vel_median, vel_std, vel_count, " +\
                      "mag_glatc, mag_gltc, mag_gazmc) " +\
                      "VALUES (?, ?, ?, ?, ?, ?)"
	elif coords == "geo":
	    command = "INSERT OR IGNORE INTO {tb2} (vel_mean, vel_median, vel_std, vel_count, " +\
                      "geo_glatc, geo_gltc, geo_gazmc) " +\
                      "VALUES (?, ?, ?, ?, ?, ?)"
	command = command.format(tb2=output_table)
	for rw in rows:
            vel_mean, vel_std, vel_count, lat, lt, azm = rw

            # find median and std
            if coords == "mlt":
                command_tmp = "SELECT vel FROM {tb1} " +\
                              "WHERE mag_glatc={lat} and mag_gltc={lt} and "+\
                              "mag_gazmc={azm}"
            elif coords == "geo":
                command_tmp = "SELECT vel FROM {tb1} " +\
                              "WHERE geo_glatc={lat} and geo_gltc={lt} and "+\
                              "geo_gazmc={azm}"
            command_tmp = command_tmp.format(tb1=input_table, lat=lat, lt=lt,
                                             azm=azm)
            try:
                cur.execute(command_tmp)
            except Exception, e:
                logging.error(e, exc_info=True)

            vels_tmp = cur.fetchall()
            vels_tmp = [x[0] for x in vels_tmp]
            vel_median = np.median(vels_tmp)
            vel_std = np.std([x for x in vels_tmp if np.abs(x) < 500.])
            if np.isnan(vel_std):
                vel_std = 500.

	    # insert the data
	    try:
		cur.execute(command,
                            (round(vel_mean,2), round(vel_median,2), round(vel_std,2),
                             vel_count, lat, lt, azm))
	    except Exception, e:
		logging.error(e, exc_info=True)

    # commit the change
    try:
	conn.commit()
    except Exception, e:
	logging.error(e, exc_info=True)

    # close db connections
    conn.close()

    return

def main(master_table=True, superposed_master_table=False, master_summary_table=True):

    # input parameters
    ftype="fitacf"
    coords="mlt"
    filtered_interval = 2.
    dbdir = "../data/sqlite3/"
    input_dbname = "sd_" + str(int(filtered_interval)) + "_min_median_" +\
                   coords + "_" + ftype + ".sqlite"
    output_dbname = "sd_master_" + coords + "_" + ftype + ".sqlite"

    input_table_1 = "all_radars"
    output_table_1 = "master_all_radars"
    input_table_2 = "master_all_radars" 
    output_table_2 = "master_summary_all_radars"

    IMF_turning = "northward"
    #IMF_turning = "southward"
    event_status = "good"
    output_table = "master_superposed_epoch_" + IMF_turning
    imf_lagtime = 15
    half_interval_length = 60

    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/superposed_master_table" + ".log",
                        level=logging.INFO)

    if master_table:
        # build a master table 
        print "building a master table"
        build_master_table(input_table_1, output_table_1,
                           ftype=ftype, coords=coords,
                           dbdir=dbdir, input_dbname=input_dbname,
                           output_dbname=output_dbname)
        print "A master table has been built"

    if superposed_master_table:
        df_events = build_event_database(IMF_turning=IMF_turning, event_status=event_status)
        build_superposed_master_table(output_table, df_events=df_events, half_interval_length=half_interval_length,
                                      imf_lagtime=imf_lagtime, ftype=ftype, coords=coords,
                                      dbdir=dbdir, input_dbname=None, output_dbname=None)


    if master_summary_table:
        # build a summary table
        print "building a master_summary table"

        master_summary(input_table_2, output_table_2, coords=coords,
                       dbdir=dbdir, db_name=output_dbname)

        print "A master_summary has been built"

    return

if __name__ == "__main__":
    #main(master_table=True, master_summary_table=False)
    #main_imf(master_table=False, master_summary_table=True)
    main(master_table=False, superposed_master_table=True, master_summary_table=False)
