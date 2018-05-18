import json
import sqlite3
import datetime as dt
import pandas as pd
import numpy as np

def sddb_to_tecdb(rad, sd_db="sd_gridded_los_data_fitacf.sqlite", 
                  tec_db="tec_at_rangecell.sqlite", dbdir="../data/sqlite3/"): 

    """Moves some columns from SD LOS data DB to TEC DB"""

    # Create db connections
    conn_sd = sqlite3.connect(dbdir + sd_db, detect_types=sqlite3.PARSE_DECLTYPES)
    conn_tec = sqlite3.connect(dbdir + tec_db, detect_types=sqlite3.PARSE_DECLTYPES)
    cur_sd = conn_sd.cursor()
    cur_tec = conn_tec.cursor()

    # create a table in the output db
    table_name = rad
    command = "CREATE TABLE IF NOT EXISTS {tb} (" +\
              "datetime TIMESTAMP, "+\
              "bmnum INTEGER, " +\
              "geo_latc TEXT, geo_lonc TEXT," +\
              "PRIMARY KEY(datetime, bmnum))"
    command = command.format(tb=table_name)
    cur_tec.execute(command)

    # Move data from SD DB to TEC DB
    command = "SELECT datetime, bmnum, geo_latc, geo_lonc FROM {tb_sd}"
    command = command.format(tb_sd=rad)
    df = pd.read_sql(command, conn_sd)
    df.to_sql(rad, conn_tec, if_exists="append", index=False)
     
    # Close db connection
    conn_sd.close()
    conn_tec.close()

def geo_to_mag(rad, stm=None, etm=None,
               dbdir="../data/sqlite3/", db_name=None,
               t_c_alt=300.):

    """ converts latc and lonc from GEO to MLAT-MTON coords.

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
    db_name : str, default to None
        Name of the sqlite db to which geo to mag conversion will be written
    t_c_alt : float, default to 300. [km]
        The altitude need to calculate the target coords (in this case, mlat-mlon coords)

    Returns
    -------
    Nothing
    """

    from davitpy.utils.coordUtils import coord_conv
    from datetime import date
    import sys
    sys.path.append("../")
    import logging

    # make db connection
    if db_name is None:
        db_name = "tec_at_rangecell.sqlite"
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

    # add new columns for mag_latc and mag_lonc
    try:
        command ="ALTER TABLE {tb} ADD COLUMN mag_latc TEXT".format(tb=table_name)
        cur.execute(command)
    except:
        # pass if the column mag_latc exists
        pass
    try:
        command ="ALTER TABLE {tb} ADD COLUMN mag_lonc TEXT".format(tb=table_name)
        cur.execute(command)
    except:
        # pass if the column mag_lonc exists
        pass

    # do the convertion to all the data in db if stm and etm are all None
    if (stm is not None) and (etm is not None):
        command = "SELECT geo_latc, geo_lonc, datetime FROM {tb} " +\
                  "WHERE datetime BETWEEN '{sdtm}' AND '{edtm}' ORDER BY datetime"
        command = command.format(tb=table_name, sdtm=stm, edtm=etm)

    # do the convertion to the data between stm and etm if any of them is None
    else:
        command = "SELECT geo_latc, geo_lonc, datetime FROM {tb} ORDER BY datetime"
        command = command.format(tb=table_name)
    try:
        cur.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)
    rows = cur.fetchall() 

    # do the conversion row by row
    if rows:
        for row in rows:
            latc, lonc, date_time= row
            if latc:
                # Load json string
                latc = json.loads(latc)
                lonc = json.loads(lonc)

                # convert from geo to mag coords
                lonc, latc = coord_conv(lonc, latc, "geo", "mag",
                                        altitude=t_c_alt,
                                        date_time=date_time)


                lonc = [(round(x,1))%360 for x in lonc]
                latc = [round(x,1) for x in latc]

                # convert to string
                latc = json.dumps(latc)
                lonc = json.dumps(lonc)
                
                # Add to db
                command = "UPDATE {tb} SET " +\
                          "mag_latc='{latc}', mag_lonc='{lonc}' " +\
                          "WHERE datetime = '{dtm}'"
                command = command.format(tb=table_name, latc=latc,
                                         lonc=lonc, dtm=date_time)

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

def add_tec_at_rangecell(rad, stm=None, etm=None, 
                         input_db="med_filt_tec.sqlite", 
                         output_db="tec_at_rangecell.sqlite",
                         dbdir="../data/sqlite3/"):

    """Add TEC values for each ragne-cell"""

    import sys
    sys.path.append("../")
    import logging

    # Create db connections
    conn_in = sqlite3.connect(dbdir + input_db, detect_types=sqlite3.PARSE_DECLTYPES)
    conn_out = sqlite3.connect(dbdir + output_db, detect_types=sqlite3.PARSE_DECLTYPES)
    cur_in = conn_in.cursor()
    cur_out = conn_out.cursor()

    input_table = "med_filt_tec"
    output_table = rad

    # add new columns for mag_latc and mag_lonc
    try:
        command ="ALTER TABLE {tb} ADD COLUMN tec TEXT".format(tb=output_table)
        cur_out.execute(command)
    except:
        # pass if the column tec exists
        pass

    # Select the data between stm and etm if both of them are not None
    if (stm is not None) and (etm is not None):
        command = "SELECT mag_latc, mag_lonc, datetime FROM {tb} " +\
                  "WHERE datetime BETWEEN '{sdtm}' AND '{edtm}' ORDER BY datetime"
        command = command.format(tb=output_table, sdtm=stm, edtm=etm)

    # Select all the data in db if any of stm and etm is None
    else:
        command = "SELECT mag_latc, mag_lonc, datetime FROM {tb} ORDER BY datetime"
        command = command.format(tb=output_table)
    try:
        cur_out.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)
    rows = cur_out.fetchall() 

    # do the conversion row by row
    if rows:
        for row in rows:
            latc, lonc, date_time= row
            if latc:
                # Load json string
                latc = json.loads(latc)
                lonc = json.loads(lonc)

                # Find the time that is closed to the datetime of interest
                dtm_tmp = date_time.replace(minute=5*int(date_time.minute/5))
                dtm_tmp = dtm_tmp.replace(microsecond=0)

                tecc = []
                for i in range(len(latc)):
                    latc_i = round(latc[i])
                    lonc_i = (np.floor(lonc[i]) + np.floor(lonc[i]) % 2) % 360
                    command = "SELECT med_tec FROM {tb} " +\
                              "WHERE mlat={latc_i} AND mlon={lonc_i} " +\
                              "AND datetime = '{dtm_tmp}'"
                    command = command.format(tb=input_table, latc_i=latc_i,
                                             lonc_i=lonc_i, dtm_tmp=dtm_tmp)
                    cur_in.execute(command)
                    rw = cur_in.fetchall()
                    if rw:
                        tecc.append(rw[0])
                    else:
                        tecc.append(np.nan)

                tecc = json.dumps(tecc)

                # do the update
                command = "UPDATE {tb} SET " +\
                          "tec='{tecc}' " +\
                          "WHERE datetime = '{dtm}'"
                command = command.format(tb=output_table, tecc=tecc,
                                         dtm=date_time)
                try:
                    #import pdb
                    #pdb.set_trace()
                    cur_out.execute(command)
                except Exception, e:
                    logging.error(e, exc_info=True)

            else:
                continue
            print("Finished " + str(date_time) + " of " + rad)
        
        # commit the results
        try:
            conn_out.commit()
        except Exception, e:
            logging.error(e, exc_info=True)

    # Close db connection
    conn_in.close()
    conn_out.close()

    return

if __name__ == "__main__":

    rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]
    rads = ["wal"]

    for rad in rads:
        # Moves some columns from SD LOS data DB to TEC DB
        print ("moving sd data to tec db for " + rad)
#        sddb_to_tecdb(rad, sd_db="sd_gridded_los_data_fitacf.sqlite", 
#                      tec_db="tec_at_rangecell.sqlite", dbdir="../data/sqlite3/") 

#        # GEO to MAG
#        print ("GEO to MAG for " + rad)
#        geo_to_mag(rad, stm=None, etm=None,
#                   dbdir="../data/sqlite3/", db_name=None,
#                   t_c_alt=300.)

        # Add TEC values for each rage-cell
        add_tec_at_rangecell(rad, stm=None, etm=None, 
                             input_db="med_filt_tec.sqlite", 
                             output_db="tec_at_rangecell.sqlite")

