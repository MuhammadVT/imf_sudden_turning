def combine_xxx_min_median(rads, ftype="fitacf", coords="mlt", 
                           filtered_interval=2.,
                           dbdir="../data/sqlite3/", db_name=None):

    """ combines xxx-minute median filtered gridded data from radars 
    specified by rads argument into a single table.

    Parameters
    ----------
    rads : list
        A list of three-letter radar codes
    ftype : str
        SuperDARN file type
    coords : str
        The Coordinate system. Default to "mlt. Can be "geo" as well.
    config_filename: str
        name and path of the configuration file
    section: str, default to "midlat"
        section of database configuration
    db_name : str, default to None
        Name of the MySQL db where xxx-min median data is stored.

    Returns
    -------
    Nothing
    """

    import numpy as np 
    import datetime as dt
    import logging
    import sqlite3

    # consruct a db name and a table name
    if db_name is None:
        db_name = "sd_" + str(int(filtered_interval)) + "_min_median_" +\
                  coords + "_" + ftype + ".sqlite"
    output_table = "all_radars"

    # make a connection to xxx-min median db
    try:
        conn = sqlite3.connect(dbdir + db_name,
                               detect_types = sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
    except Exception, e: 
        logging.error(e, exc_info=True)

    # create a table that combines gridded data from all the radar
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
        cur.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)

    # construct table names in iscat db
    tbl_names = rads

    # move the data between tables 
    for i, tbl_name in enumerate(tbl_names):
        rad = tbl_name
        if coords == "mlt":
            command = "SELECT vel, mag_glatc, mag_gltc, mag_gazmc, " +\
                      "datetime FROM {tb1} ORDER By datetime ASC"
        elif coords == "geo":
            command = "SELECT vel, geo_glatc, geo_gltc, geo_gazmc, " +\
                      "datetime FROM {tb1} ORDER By datetime ASC"
        command = command.format(tb1=tbl_name)

        # fetch the data
        try:
            cur.execute(command)
        except Exception, e:
            logging.error(e, exc_info=True)
        rows = cur.fetchall()

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
                vel, lat, lt, azm, dtm = rw

                try:
                    cur.execute(command, (vel, lat, lt, azm, dtm, rad))
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

def main():
    """executes the above function."""

    import datetime as dt
    import logging


    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/combine_xxx_min_median.log",
                        level=logging.INFO)
 
    # input parameters
    rads= ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]

    ftype="fitacf"
    coords="mlt"
    db_name=None
    dbdir = "../data/sqlite3/"
    filtered_interval = 2.

    # take xxx minutes median values
    print("moving xxx_min_median of " + str(rads) + " into all_radars table")
    t1 = dt.datetime.now()
    combine_xxx_min_median(rads, ftype=ftype, coords=coords,
                           filtered_interval=filtered_interval,
                           dbdir=dbdir, db_name=db_name)
    t2 = dt.datetime.now()
    print("Finished moving. It took " + str((t2-t1).total_seconds() / 60.) + " mins\n")

    return

if __name__ == "__main__":
    main()

