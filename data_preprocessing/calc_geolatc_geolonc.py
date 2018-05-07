"""
written by Muhammad Rafiq, 2018-05-07
"""

import datetime as dt
from davitpy.pydarn.radar.radFov import slantRange, calcFieldPnt, calcAzOffBore
from davitpy.pydarn.radar.radStruct import site 
from davitpy.utils.coordUtils import coord_conv

import pdb


class latc_lonc_to_db(object):

    def __init__(self, rad, stm, etm, ftype="fitacf",
		 dbdir="../data/sqlite3/", db_name=None):

        """ calculates the center points of range-beam cells of a given radar
        in geo coords and add them into a new db.

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

        """ 

        rad_id_dict = {"bks":33, "wal":32, "cve":207, "cvw":206,
                       "fhe":205, "fhw":204, "ade":209, "adw":208,
                       "hok":40, "hkw":41, "tig":14, "unw":18, "bpk":24 }
        self.rad = rad
        self.rad_id = rad_id_dict[rad]
        self.stm = stm
        self.etm = etm
        self.ftype = ftype
	self.dbdir = dbdir
        self.db_name = db_name
        self.table_name = self.rad
        self.conn = self._create_dbconn()
        self.sites = self._create_site_list()

    def _create_dbconn(self):

	""" creates a db connection

        Parameters
        ----------
        db_name : str, default to None
            Name of the SQLITE db to which geolat , geolon will be written

	"""
	import sqlite3
        # make a db connection
        conn = sqlite3.connect(self.dbdir + self.db_name,
			       detect_types = sqlite3.PARSE_DECLTYPES)

        return conn

    def _create_site_list(self):

        """ creats a list of sites for a given self.rad for the period between
        self.stm and self.etm """

        import sqlite3

        # create a sqlite3 db connection to the radar.sqlite3
        conn = sqlite3.connect(database="../data/sqlite3/radars.sqlite",
                               detect_types = sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()

        # select all the datetime values (tval) later than stm
        command = "SELECT tval FROM hdw WHERE id=? "
        command = '{:s}and tval>=? ORDER BY tval ASC'.format(command)
        cur.execute(command, (self.rad_id, self.stm))
        tvals_stm = cur.fetchall()
        tvals_stm = [x[0] for x in tvals_stm]

        # select all the datetime values (tval) later than etm
        command = "SELECT tval FROM hdw WHERE id=? "
        command = '{:s}and tval>=? ORDER BY tval ASC'.format(command)
        cur.execute(command, (self.rad_id, self.etm))
        tval_etm = cur.fetchone()[0]
        indx_etm = tvals_stm.index(tval_etm)

        # select the tvals of interest
        tvals = tvals_stm[:indx_etm+1]

        site_list = []
        for tval in tvals:
            site_list.append(site(code=self.rad, dt=tval))
        return site_list


    def add_latclonc_to_db(self):
        """ calculates latc and lonc of each range-beam cell in 'geo'
        coordinates and update them into a newly copied table.
        If self.table_name does not exist in the db, it will not do anything"""

        import logging
	import json
        
        cur = self.conn.cursor()

        # add new columns
        try:
            command ="ALTER TABLE {tb} ADD COLUMN geo_latc TEXT".format(tb=self.table_name) 
            cur.execute(command)
        except:
            # pass if the column geo_latc exists
            pass
        try:
            command ="ALTER TABLE {tb} ADD COLUMN geo_lonc TEXT".format(tb=self.table_name) 
            cur.execute(command)
        except:
            # pass if the column geo_lonc exists
            pass

        # iterate through tvals of the self.sites
        sdtm = self.stm
        for ii, st in enumerate(self.sites):
            if ii == len(self.sites)-1:
                edtm = self.etm
            else:
                edtm = st.tval

            # select data for the period between sdtm and edtm
            command = "SELECT slist, vel, bmnum, frang, rsep, datetime " +\
                      "FROM {tb} WHERE datetime BETWEEN '{sdtm}' AND '{edtm}' "+\
                      "ORDER BY datetime"
            command = command.format(tb=self.table_name, sdtm=str(sdtm), edtm=str(edtm))
            try:
                cur.execute(command)
            except Exception, e:
                logging.error(e, exc_info=True)
            rows = cur.fetchall() 

            if rows != []:
                # loop through rows 
                for row in rows:
                    slist, vel, bmnum, frang, rsep, date_time = row
		    # calculate latc_all and lonc_all in 'geo' coords
		    latc_all, lonc_all = calc_latc_lonc(self.sites[ii],
							bmnum, frang, rsep, 
							altitude=300., elevation=None,
							date_time=None)
                        
		    vel = json.loads(vel)
		    slist = json.loads(slist)

                    # exclude the slist values beyond maxgate and their correspinding velocities
                    vel = [vel[i] for i in range(len(vel)) if slist[i] < st.maxgate]
                    slist = [s for s in slist if s < st.maxgate]

                    # extract latc and lonc values
                    latc = [round(latc_all[s],2) for s in slist]
                    lonc = [round(lonc_all[s], 2) for s in slist]

                    # convert to comma seperated text
		    slist = json.dumps(slist)
		    vel = json.dumps(vel)
		    latc = json.dumps(latc)
		    lonc = json.dumps(lonc)

                    # update the table
                    command = "UPDATE {tb} SET slist='{slist}', vel='{vel}', " +\
                              "geo_latc='{latc}', geo_lonc='{lonc}' WHERE datetime = '{dtm}'"
                    command = command.format(tb=self.table_name, slist=slist, vel=vel,\
                                             latc=latc, lonc=lonc, dtm=date_time)
                    try:
                        cur.execute(command)
                    except Exception, e:
                        logging.error(e, exc_info=True)

            # update sdtm
            sdtm = edtm

        # commit the data into the db
        try:
            self.conn.commit()
        except Exception, e:
            logging.error(e, exc_info=True)

        # close db connection
        self.conn.close()
            
        return

def calc_latc_lonc(site, bmnum, frang, rsep, altitude=300.,
                   elevation=None,
                   date_time=None):

    """ calculates center lat and lon of all the range-gates of a given bmnum
    
    Parameters
    ----------
    site : davitpy.pydarn.radar.radStruct.site object
    bmnum : int
	bmnum argument only works in search_allbeams is set to False
    frang : int 
        Distance at which the zero range-gate starts [km]
    rsep : int
        Range seperation [km]
    altitude : float
        Default to 300. [km]
    elevation : float
        Defalut to None, in which case it will be estimated by the algorithm.
    date_time : datetime.datetime
        the datetime for which the FOV is desired. Required for mag and mlt,
        and possibly others in the future. Default: None

    Returns
    -------
    two lists
        Calculated center latitudes and longitudes of range gates of a given beam
    
    """
    import numpy as np

    # initialze papameters
    nbeams = site.maxbeam
    ngates = site.maxgate
    bmsep = site.bmsep
    recrise = site.recrise
    siteLat = site.geolat
    siteLon = site.geolon
    siteAlt = site.alt
    siteBore = site.boresite
    gates = np.arange(ngates)

    # Create output arrays
    lat_center = np.zeros(ngates, dtype='float')
    lon_center = np.zeros(ngates, dtype='float')

    # Calculate deviation from boresight for center of beam
    boff_center = bmsep * (bmnum - (nbeams - 1) / 2.0)

    # Calculate center slant range
    srang_center = slantRange(frang, rsep, recrise,
                              gates, center=True)

    # Calculate coordinates for Center of the current beam
    for ig in gates:
        talt = altitude
        telv = elevation

        # calculate projections
        latc, lonc = calcFieldPnt(siteLat, siteLon, siteAlt * 1e-3,
                                  siteBore, boff_center,
                                  srang_center[ig], elevation=telv,
                                  altitude=talt, model="IS",
                                  fov_dir="front")

        # Save into output arrays
        lat_center[ig] = latc
        lon_center[ig] = lonc

    return lat_center, lon_center


def worker(rad, stm, etm, ftype="fitacf", 
   	   dbdir="../data/sqlite3/", db_name=None):

    import datetime as dt
    import sys

    # create a latc_lonc_to_db object
    t1 = dt.datetime.now()
    print("creating an latc_lonc_to_db object for " + \
          rad + " for period between " + str(stm) + " and " + str(etm))
    obj = latc_lonc_to_db(rad, stm, etm, ftype=ftype,
			  dbdir=dbdir, db_name=db_name)

    # calculate geolatc and geolonc and write them into a db
    obj.add_latclonc_to_db()
    print("geolatc and geolonc have been written to db for "  +\
           rad + " for period between " + str(stm) + " and " + str(etm))

    t2 = dt.datetime.now()
    print("Finishing an latc_lonc_to_db object for " +\
           rad + " for period between " + str(stm) + " and " +\
           str(etm) + " took " + str((t2-t1).total_seconds() / 60.) + " mins\n")
    
    return

def main(run_in_parallel=True):
    """ Call the functions above. Acts as an example code.
    Multiprocessing has been implemented to do parallel computing.
    A unit process is for a radar (i.e. a db table)"""

    import datetime as dt
    import multiprocessing as mp
    from shutil import copyfile 
    import sys
    sys.path.append("../")
    import logging

    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/calc_geolatc_geolonc.log",
                        level=logging.INFO)

    # input parameters
#    stm = dt.datetime(2011, 1, 1)     # includes sdate
#    etm = dt.datetime(2017, 1, 1)     # does not include etm

    stm = dt.datetime(2014, 12, 16, 13)     # includes sdate
    etm = dt.datetime(2014, 12, 16, 15)     # does not include etm

    # run the code for the following radars in parallel
    rad_list = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]


    ftype = "fitacf"
    dbdir="../data/sqlite3/"
    # Copy db
    old_dbname = "sd_los_data_" + ftype + ".sqlite"
    db_name = "sd_gridded_los_data_" + ftype + ".sqlite"
    copyfile(dbdir + old_dbname, dbdir + db_name)

    # loop through the rads
    for rad in rad_list:

        # store the multiprocess
        procs = []

	if run_in_parallel:

	    # cteate a process
	    worker_kwargs = {"ftype":ftype, "db_name":db_name, "dbdir":dbdir}
	    p = mp.Process(target=worker, args=(rad, stm, etm),
			   kwargs=worker_kwargs)
	    procs.append(p)

	    # run the process
	    p.start()

	else:
	    worker(rad, stm, etm, ftype=ftype, dbdir=dbdir, db_name=db_name)

	if run_in_parallel:
	    # make sure the processes terminate
	    for p in procs:
		p.join()

    return

if __name__ == '__main__':
    main(run_in_parallel=False)
