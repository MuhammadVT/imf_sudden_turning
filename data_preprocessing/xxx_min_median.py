def xxx_min_median(rad, stm, etm, ftype="fitacf", filtered_interval=2., 
		   coords="mlt", dbdir="../data/sqlite3/",
                   input_dbname=None, output_dbname=None):
    
    """ Bins the gridded data from a single radar into xxx-minute intervals.
    e.g., at each xxx-minute interval, median vector in each azimuth bin within a grid cell is
    selected as the representative velocity for that bin. 
    The results are stored in a different db such that data from a given radar
    are written into a single table named by the radar name.

    Parameters
    ----------
    rad : str
        Three-letter radar code
    stm : datetime.datetime
        The start time.
    etm : datetime.datetime
        The end time.
    ftype : str
        SuperDARN file type
    coords : str
        Coordinates in which the binning process takes place.
        Default to "mlt. Can be "geo" as well. 
    input_dbname : str, default to None
        Name of the sqlite db to which gridded data has been written
    output_dbname : str, default to None
        Name of the sqlite db to which xxx-min median filtered data will be written

    Returns
    -------
    Nothing

    """

    import numpy as np
    import datetime as dt
    import logging
    import sqlite3
    import json

    # create db names
    if input_dbname is None:
        input_dbname = "sd_gridded_los_data_" + ftype + ".sqlite"
    if output_dbname is None:
        output_dbname = "sd_xxx_min_median_" + coords + "_" + ftype + ".sqlite"

    # make a connection to gridded los db
    try:
        conn_in = sqlite3.connect(dbdir + input_dbname,
                                  detect_types = sqlite3.PARSE_DECLTYPES)
        cur_in = conn_in.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)
 
    # make a connection to xxx-min median filtered data 
    try:
        conn_out = sqlite3.connect(dbdir + output_dbname,
                                  detect_types = sqlite3.PARSE_DECLTYPES)
        cur_out = conn_out.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    input_table = rad
    output_table = rad
    # create a table
    if coords == "mlt":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel float(9,2)," +\
                  " mag_glatc float(7,2)," +\
                  " mag_gltc float(8,2)," +\
                  " mag_gazmc SMALLINT," +\
                  " datetime DATETIME, " +\
                  " CONSTRAINT xxx_min PRIMARY KEY (" +\
		  "mag_glatc, mag_gltc, mag_gazmc, datetime))"

    elif coords == "geo":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel float(9,2)," +\
                  " geo_glatc float(7,2)," +\
                  " geo_gltc float(8,2)," +\
                  " geo_gazmc SMALLINT," +\
                  " datetime DATETIME, " +\
                  " CONSTRAINT xxx_min PRIMARY KEY (" +\
		  "geo_glatc, geo_gltc, geo_gazmc, datetime))"
    command = command.format(tb=output_table)
    try:
        cur_out.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)

    # initial starting and ending time of the time interval given by filtered_interval 
    sdtm = stm
    edtm = sdtm + dt.timedelta(minutes=filtered_interval)

    # slide the xxx-minute window from stm to etm
    while edtm <= etm:
        # bin_vel stores the velocity data as {glatc-glonc-gazmc: [velocites]}  
        bin_vel = {}
        # select column variables for a xxx-minute interval
        if coords == "mlt":
            command = "SELECT vel, mag_glatc, mag_gltc, mag_gazmc " +\
                      "FROM {tb} WHERE datetime BETWEEN '{sdtm}' AND '{edtm}'"
        elif coords == "geo":
            command = "SELECT vel, geo_glatc, geo_gltc, geo_gazmc " +\
                      "FROM {tb} WHERE datetime BETWEEN '{sdtm}' AND '{edtm}'"
        command = command.format(tb=input_table, sdtm=sdtm, edtm=edtm)

        try:
            cur_in.execute(command)
        except Exception, e:
            logging.error(e, exc_info=True)
        rows_tmp = cur_in.fetchall()

        if rows_tmp:
            # loop through each row
            for row in rows_tmp:
                vel, lat, lon, az = row
                if None not in row:
                    # convert from string to float
                    vel = json.loads(vel)
                    lat = json.loads(lat)
                    lon = json.loads(lon)
                    az = json.loads(az)

                    for i in range(len(vel)):
                        # exclude NaN elements
                        if np.isnan(lat[i]):
                            continue

                        # build xxx bin_vel dict ({glatc-glonc-gazmc: [velocites]})
                        try:
                            bin_vel[(lat[i],lon[i],az[i])].append(vel[i])
                        except KeyError:
                            bin_vel[(lat[i],lon[i],az[i])] = [vel[i]]
                else:
                    continue
        
        else:
            # update starting and ending time of the time interval given by filtered_interval
            sdtm = edtm
            edtm = sdtm + dt.timedelta(minutes=filtered_interval)
            continue

        if bin_vel:
            # take the mid-point of sdtm and edtm
            mid_tm = sdtm + dt.timedelta(minutes=filtered_interval/2.)
                
            # populate the rad table 
            for ky in bin_vel.keys(): 
                # take the median value
                bin_vel[ky] = round(np.median(bin_vel[ky]),2)
                if coords == "mlt":
                    command = "INSERT OR IGNORE INTO {tb} (vel, mag_glatc, mag_gltc, " +\
                              "mag_gazmc, datetime) VALUES (?, ?, ?, ?, ?)"
                elif coords == "geo":
                    command = "INSERT OR IGNORE INTO {tb} (vel, geo_glatc, geo_gltc, " +\
                              "geo_gazmc, datetime) VALUES (?, ?, ?, ?, ?)"
                command = command.format(tb=output_table)

                try:
                    cur_out.execute(command,
                                    (bin_vel[ky], ky[0], ky[1], ky[2], mid_tm))
                except Exception, e:
                    logging.error(e, exc_info=True)

        print("finished median filtering for " + rad  +\
              " for time interval between " + str(sdtm) + " and " + str(edtm))

        # update starting and ending time of the time interval given by filtered_interval
        sdtm = edtm
        edtm = sdtm + dt.timedelta(minutes=filtered_interval)

    # commit the change
    try:
        conn_out.commit()
    except Exception, e:
        logging.error(e, exc_info=True)

    # close db connections
    conn_in.close()
    conn_out.close()

    return

def worker(rad, stm, etm, ftype="fitacf", coords="mlt",
           filtered_interval=2., dbdir="../data/sqlite3/",
           input_dbname=None, output_dbname=None):

    import datetime as dt

    # take xxx-minute median values
    print("start working on table " + rad + " for interval between " +\
          str(stm) + " and " + str(etm))
    xxx_min_median(rad, stm, etm, ftype=ftype, coords=coords,
                   filtered_interval=filtered_interval,
                   dbdir=dbdir, input_dbname=input_dbname,
                   output_dbname=output_dbname)
    print("finish taking xxx mimute median filtering on " + rad +\
          " for interval between " + str(stm) + " and " + str(etm))

    return

def main(run_in_parallel=True):
    """ Call the functions above. Acts as an example code.
    Multiprocessing has been implemented to do parallel computing.
    A unit process runs for a radar"""

    import datetime as dt
    import multiprocessing as mp
    import logging
    
    # create a log file to which any error occured between client and
    # MySQL server communication will be written.
    logging.basicConfig(filename="./log_files/xxx_min_median.log",
                        level=logging.INFO)
    # input parameters
    stm = dt.datetime(2014, 12, 16) 
    etm = dt.datetime(2014, 12, 17) 

    ftype = "fitacf"
    coords = "mlt"
    dbdir = "../data/sqlite3/"
    input_dbname = None       # if set to None, default db would be used. 
    output_dbname = None       # if set to None, default xxx_min_median db would be used. 
    filtered_interval=2.

    # run the code for the following radars in parallel
    rad_list = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]
    
    # store the multiprocess
    procs = []
    # loop through radars
    for rad in rad_list: 
	if run_in_parallel:
	    # cteate a process
	    worker_kwargs = {"ftype":ftype, "coords":coords,
                             "filtered_interval":filtered_interval,
			     "dbdir":dbdir, "input_dbname":input_dbname,
			     "output_dbname":output_dbname}
	    p = mp.Process(target=worker, args=(rad, stm, etm),
			   kwargs=worker_kwargs)
	    procs.append(p)
	    
	    # run the process
	    p.start()
	    
	else:
	    # run in serial
            worker(rad, stm, etm, ftype=ftype, 
                   coords=coords, filtered_interval=filtered_interval,
                   dbdir=dbdir, input_dbname=input_dbname,
                   output_dbname=output_dbname)
            
    if run_in_parallel:
        # make sure the processes terminate
        for p in procs:
            p.join()

    return

if __name__ == "__main__":
    import datetime as dt
    t1 = dt.datetime.now()
    main(run_in_parallel=False)
    t2 = dt.datetime.now()
    print("Finishing xxx-min median filtering took " +\
    str((t2-t1).total_seconds() / 60.) + " mins\n")

