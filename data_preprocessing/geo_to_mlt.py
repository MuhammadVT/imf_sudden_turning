# radar locations in geo coords
rad_loc_dict = {"bks" : (37.10, -77.95), "wal" : (37.93, -75.47),
                "fhe" : (38.86, -99.39), "fhw" : (38.86, -99.39),
                "cve" : (43.27, -120.36), "cvw" : (43.27, -120.36),
                "ade" : (51.89,	-176.63), "adw" : (51.89, -176.63),
	        "hok" : (43.53,	143.61), "hkw" : (43.54, 143.61),
	 	"tig" : (-43.38, 147.23), "unw" : (-46.51, 168.38),
	 	"bpk" : (-34.62, 138.46)}

import pdb

def geo_to_mlt(rad, stm=None, etm=None, ftype="fitacf",
	       dbdir="../data/sqlite3/", db_name=None,
               t_c_alt=300., stay_in_geo=False):

    """ converts latc and lonc from GEO to MLAT-MLT coords (MLT is in degrees).
    Also calcuates the azmimuthal velocity angle (in degrees) relative to the magnetic pole.
    NOTE : if stay_in_geo is set to False then origional latc, lonc do not change but 
    local time, ltc, in degrees (e.g. 0 (or 360) degree is midnight, 
    180 degrees is noon time) will be added. 

    Parameters
    ----------
    rad : str
        Three-letter radar code
    stm : datetime.datetime
        The start time.
        Default to None, in which case takes the earliest in db.
    etm : datetime.datetime
        The end time.
        Default to None, in which case takes the latest time in db.
        NOTE: if stm is None then etm should also be None, and vice versa.
    ftype : str
        SuperDARN file type
    db_name : str, default to None
	Name of the sqlite db to which geo to mlt conversion will be written
    t_c_alt : float, default to 300. [km]
        The altitude need to calculate the target coords (in this case, mlat-mlt coords)
    stay_in_geo : bool
        if set to True no coord conversion is done. Calculation would be in geo, and
        the original UT time will be converted into local time.

    Returns
    -------
    Nothing
    """

    from davitpy.utils.coordUtils import coord_conv
    import datetime as dt
    from datetime import date
    import sqlite3
    import json
    import sys
    sys.path.append("../")
    import logging

    # make db connection
    if db_name is None:
        db_name = "sd_gridded_los_data_" + ftype + ".sqlite"
    try:
        conn = sqlite3.connect(dbdir + db_name,
                               detect_types = sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
    except Exception, e:
	logging.error(e, exc_info=True)

    table_name = rad 
    # Check whether the table of interest exists
    command = "SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    command = command.format(table_name = table_name)
    cur.execute(command)
    if not cur.fetchall():
	return

    # add new columns for geo_ltc (local time in geo) 
    if stay_in_geo:
        try:
            command ="ALTER TABLE {tb} ADD COLUMN geo_ltc TEXT".format(tb=table_name)
            cur.execute(command)
        except:
            # pass if the column geo_ltc exists
            pass

    # add new columns for mag_latc and mag_ltc
    else:
        try:
            command ="ALTER TABLE {tb} ADD COLUMN mag_latc TEXT".format(tb=table_name)
            cur.execute(command)
        except:
            # pass if the column mag_latc exists
            pass
        try:
            command ="ALTER TABLE {tb} ADD COLUMN mag_ltc TEXT".format(tb=table_name)
            cur.execute(command)
        except:
            # pass if the column mag_ltc exists
            pass
    # add a new column for LOC velocity azm angle (in degrees)
    try:
        # add the azmimuthal velocity angle (azmc) relative to the magnetic (or geo) pole
        if stay_in_geo:
            command ="ALTER TABLE {tb} ADD COLUMN geo_azmc TEXT".format(tb=table_name) 
        else:
            command ="ALTER TABLE {tb} ADD COLUMN mag_azmc TEXT".format(tb=table_name) 
        cur.execute(command)
    except:
        # pass if the column geo_azmc or mag_azmc exists
        pass

    # add new columns for beam azm angle (in degrees) in mag coords and for rad_mlt
    if not stay_in_geo:
        try:
        # add the bmazm angle relative to the magnetic (or geo) pole
            command ="ALTER TABLE {tb} ADD COLUMN mag_bmazm REAL".format(tb=table_name) 
            cur.execute(command)
        except:
            # pass if the column mag_bmazm exists
            pass

        try:
        # add the MLT location of a radar
            command ="ALTER TABLE {tb} ADD COLUMN rad_mlt REAL".format(tb=table_name) 
            cur.execute(command)
        except:
            # pass if the column rad_mlt exists
            pass


    # do the convertion to all the data in db if stm and etm are all None
    if (stm is not None) and (etm is not None):
        command = "SELECT geo_latc, geo_lonc, bmazm, datetime FROM {tb} " +\
                  "WHERE datetime BETWEEN '{sdtm}' AND '{edtm}' ORDER BY datetime"
        command = command.format(tb=table_name, sdtm=stm, edtm=etm)

    # do the convertion to the data between stm and etm if any of them is None
    else:
        command = "SELECT geo_latc, geo_lonc, bmazm, datetime FROM {tb} ORDER BY datetime"
        command = command.format(tb=table_name)
    try:
        cur.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)
    rows = cur.fetchall() 

    # do the conversion row by row
    if rows:
        for row in rows:
            latc, lonc, bmazm, date_time= row
            if latc:
                # Load json string
                latc = json.loads(latc)
                lonc = json.loads(lonc)

                # Calculates the LOS vel direction (in degrees) with respect to
                # mag (or geo) pole for each vector in each range-beam cell.
                azm_txt = geobmazm_to_magazm(rad, bmazm, latc, lonc, alt=t_c_alt,
                                               time=date_time.date(), stay_in_geo=stay_in_geo)

                if stay_in_geo:
                    lonc = [x%360 for x in lonc]

                    # convert utc to local time in degrees
                    lonc_ltm = []
                    for lonc_i in lonc:
                        lonc_tmp = lonc_i if lonc_i<=180 else lonc_i-360

                        # convert utc to local time
                        local_dt = date_time + dt.timedelta(hours=lonc_tmp/15.)
                        ltm = local_dt.time()  

                        # convert local time to degrees. e.g. 0 (or 360) degree is midnight, 
                        # 180 degrees is noon time. 
                        lonc_ltm.append((ltm.hour + ltm.minute/60. + ltm.second/3600.) * 15.)
                    lonc = lonc_ltm

                else:

                    # convert from geo to mlt degress
                    lonc, latc = coord_conv(lonc, latc, "geo", "mlt",
                                            altitude=t_c_alt,
                                            date_time=date_time)


                lonc = [(round(x,2))%360 for x in lonc]
                latc = [round(x,2) for x in latc]

                # convert to string
                latc = json.dumps(latc)
                lonc = json.dumps(lonc)
                
                # update into the db
                if stay_in_geo:
                    command = "UPDATE {tb} SET geo_ltc='{lonc}', geo_azmc='{azm_txt}'\
                               WHERE datetime = '{dtm}'"
                    command = command.format(tb=table_name, lonc=lonc,
                                             azm_txt=azm_txt, dtm=date_time)
                else:

                    # Find MLT location of a radar 
                    rad_lat, rad_lon = rad_loc_dict[rad]
                    rad_lon, rad_lat = coord_conv(rad_lon, rad_lat, "geo", "mlt",
                                                  altitude=0.,
                                                  date_time=date_time)
                    rad_lon = round(rad_lon % 360,2)
                    # Convert bmazm from geo to mag
                    mag_bmazm = geobmazm_to_magbmazm(rad, bmazm, alt=0.,
                                                     time=date_time.date(),
                                                     stay_in_geo=stay_in_geo)
                    command = "UPDATE {tb} SET mag_bmazm={mag_bmazm}, rad_mlt={rad_mlt}, " +\
                              "mag_latc='{latc}', mag_ltc='{lonc}', " +\
                              "mag_azmc='{azm_txt}' WHERE datetime = '{dtm}'"
                    command = command.format(tb=table_name, mag_bmazm=mag_bmazm, rad_mlt=rad_lon,
                                             latc=latc, lonc=lonc, azm_txt=azm_txt,
                                             dtm=date_time)

                # do the update
                try:
                    cur.execute(command)
                except Exception, e:
                    logging.error(e, exc_info=True)

            else:
                continue
        
        # commit the results
        try:
            conn.commit()
        except Exception, e:
            logging.error(e, exc_info=True)

    # close db connection
    conn.close()

    return

def geobmazm_to_magbmazm(rad, bmazm, alt=300.,
                         time=None, stay_in_geo=False):
    """ Convert bmazm from geo to mag by adding
        the magnetic declanation angle.
    bmazm : float
        bmazm of a certain beam in geo. 
        0 degree shows the geo north direction
        180 degree shows the geo south direction
    alt : float, default to 300. [km]
        altitude value at which coords. conversions take place.
    time : datetime.datetime
        Needed for geo to mlt conversion. Default to None. 
    stay_in_geo : bool
        if set to True no coords. conversion is done. Calculation would be in geo
   
    Return
    ------
    mag_bmazm : float
        bmazm (in degrees) with respect to the magnetic pole

    """
    
    from geomag import geomag
    from datetime import date
   
    rad_lat, rad_lon = rad_loc_dict[rad]
    rad_lon = rad_lon % 360
    gm = geomag.GeoMag()
    if stay_in_geo:
        new_bmazm = (round(bmazm,2)) % 360
    else:
        # convert bmazm from geo to mag by adding
        # the magnetic declanation angle
        mg = gm.GeoMag(rad_lat, rad_lon, h=alt, time=time)
        new_bmazm = (round(bmazm - mg.dec,2)) % 360

    return new_bmazm

def geobmazm_to_magazm(rad, bmazm, latc, lonc, alt=300.,
                       time=None, stay_in_geo=False):
    """ calculates the LOS vel direction (in degrees) with respect to
    mag (or geo) pole for each vector in each range-beam cell.
   
    bmazm : float
        bmazm of a certain beam in geo. 
        0 degree shows the geo north direction
        180 degree shows the geo south direction
    latc : list
        center geo latitudes of range-gates cells along a beam.
    lonc : list
        center geo longitudes of range-beam cells along a beam.
    alt : float, default to 300. [km]
        altitude value at which coords. conversions take place.
    time : datetime.datetime
        Needed for geo to mlt conversion. Default to None. 
    stay_in_geo : bool
        if set to True no coords. conversion is done. Calculation would be in geo
   
    Return
    ------
    azm_txt : string
        LOS vel. azm values (in degrees) at the positions of latc and lonc in
        mag (or geo) coords.

    """
    
    from geomag import geomag
    from datetime import date
    import numpy as np
    import json
   
    rad_lat, rad_lon = rad_loc_dict[rad]
    rad_lon = rad_lon % 360
    azm_lst = []
    gm = geomag.GeoMag()
    for i in range(len(latc)):
        # calculate the los vel angle in geo using spherical trigonometry. Then angles are defined
        # in the same way as those in spherical trigonometry section in mathworld
        #B = np.deg2rad(np.abs(bmazm))

        # catch nan value
        if np.isnan(latc[i]):
            azm_lst.append(np.nan)
            continue

        b_prime = np.deg2rad(90. - latc[i])
        a_prime = np.deg2rad(90. - rad_lat)
        AB_dellon = np.deg2rad(np.abs(lonc[i]-rad_lon))
        c_prime = np.arccos(np.sin(np.deg2rad(rad_lat)) * np.sin(np.deg2rad(latc[i])) +\
                          np.cos(np.deg2rad(rad_lat)) * np.cos(np.deg2rad(latc[i])) * np.cos(AB_dellon))
        s_prime = 1./2 * (a_prime + b_prime + c_prime)
        if round(np.rad2deg(a_prime),5) == round(np.rad2deg(s_prime),5):
            A = np.pi
        else:
            A = 2 * np.arcsin(np.sqrt((np.sin(s_prime - b_prime) * np.sin(s_prime - c_prime)) /\
                                  (np.sin(b_prime) * np.sin(c_prime))))
        losvel_dir = np.sign(bmazm) * (180 - np.rad2deg(A))
        
        if stay_in_geo:
            azm_mag = (round(losvel_dir,2)) % 360
        else:
            # convert losvel_dir from geo to mag by adding
            # the magnetic declanation angle to the los vel angle in geo
            mg = gm.GeoMag(latc[i], lonc[i], h=alt, time=time)
            azm_mag = (round(losvel_dir - mg.dec,2)) % 360
        azm_lst.append(azm_mag)

#    # convert the list entries to a comma seperated strings
#    azm_txt =",".join([str(x) for x in azm_lst])
    azm_txt = json.dumps(azm_lst)

    return azm_txt

def worker(rad, stm=None, etm=None, ftype="fitacf",
           dbdir="../data/sqlite3/", db_name=None,
           t_c_alt=300., stay_in_geo=False):

    """ A worker function to be used for parallel computing """

    import datetime as dt

    if db_name is None:
        db_name = "sd_gridded_los_data_" + ftype + ".sqlite"

    # start running geo_to_mlt
    t1 = dt.datetime.now()
    if stay_in_geo:
        print("start converting geo to geo for " +\
              rad + " for period between " + str(stm) + " and " + str(etm))
    else:
        print("start converting geo to mlt for " +\
              rad + " for period between " + str(stm) + " and " + str(etm))
    geo_to_mlt(rad, stm=stm, etm=etm, ftype=ftype,
	       dbdir=dbdir, db_name=db_name,
               t_c_alt=t_c_alt, stay_in_geo=stay_in_geo)
    print("New coords. values have been written to db for " +\
           rad + " for period between " + str(stm) + " and " + str(etm))

    t2 = dt.datetime.now()
    print("Finishing coords. conversion for " + \
           rad + " for period between " + str(stm) + " and " +\
           str(etm) + " took " + str((t2-t1).total_seconds() / 60.) + " mins\n")

    return

def main(run_in_parallel=True):
    """ Call the functions above. Acts as an example code.
    Multiprocessing has been implemented to do parallel computing.
    A unit process is for a radar beam (i.e. a db table)"""

    import datetime as dt
    import multiprocessing as mp
    import logging
    
    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/geo_to_mlt.log",
                        level=logging.INFO)
    
    # input parameters
    stm = None 
    etm = None 
    ftype = "fitacf"
    t_c_alt = 300.  # [km]
    stay_in_geo=False    # set this to True if you want to remain in "geo" coords
    db_name = None       # if set to None default sqlite db would be read. 
    dbdir="../data/sqlite3/"

    # run the code for the following radars in parallel
    #rad_list = ["tig", "unw"]
    #rad_list = ["bpk"]
    rad_list = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]
    
    # loop through the dates
    # store the multiprocess
    procs = []
    for rad in rad_list:
        if run_in_parallel:
            # cteate a process
            worker_kwargs = {"stm":stm, "etm":etm, "ftype":ftype,
                             "dbdir":dbdir, "db_name":db_name,
                             "t_c_alt":t_c_alt, "stay_in_geo":stay_in_geo}
            p = mp.Process(target=worker, args=(rad),
                           kwargs=worker_kwargs)
            procs.append(p)
            
            # run the process
            p.start()
            
        else:
            worker(rad, stm=stm, etm=etm, ftype=ftype,
                   dbdir=dbdir, db_name=db_name,
                   t_c_alt=t_c_alt, stay_in_geo=stay_in_geo)
            
    if run_in_parallel:
        # make sure the processes terminate
        for p in procs:
            p.join()

    return

if __name__ == "__main__":
    main(run_in_parallel=False)

