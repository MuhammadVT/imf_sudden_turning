import datetime as dt
import sqlite3
import multiprocessing as mp
import sys
import numpy as np
import logging

def move_to_db(stms, etms, rads, channels, db_name,
               ftype="fitacf", dbdir="../data/sqlite3/",
	       run_in_parallel=False):
    """ Reads data from a given radar """
	
    from move_sddata_to_db import worker

    # loop through the datetimes in stms
    for i in range(len(stms)):
        stm = stms[i]
        etm = etms[i]

        # loop through the radars
        procs = []
        for j, rad in enumerate(rads):
            channel = channels[j]
	    if run_in_parallel:
		# Creat a processe
		p = mp.Process(target=worker,
			       args=(db_name, dbdir, rad, stm, etm,
			       ftype, channel))
		procs.append(p)

		# Run the process
		p.start()
	    else:
		worker(db_name, dbdir, rad, stm, etm, ftype, channel)

        # Make sure the processes terminate
        for p in procs:
            p.join()

    return

def add_geolatc_geolonc(rads, stms, etms, ftype = "fitacf",
			db_name=None,
		        dbdir="../data/sqlite3/",
			run_in_parallel=False):
    
    """ Calculates latc and lonc of each range-beam cell in 'geo'
    coordinates and update them in the same table"""

    from shutil import copyfile
    import os
    from calc_geolatc_geolonc import worker

    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/calc_geolatc_geolonc.log",
                        level=logging.INFO)

#    # Copy db if not exists
#    old_dbname = "sd_los_data_" + ftype + ".sqlite"
#    db_name = "sd_gridded_los_data_" + ftype + ".sqlite"
#    if not os.path.isfile(dbdir + db_name):
#	copyfile(dbdir + old_dbname, dbdir + db_name)
    
    if db_name is None:
	db_name = "sd_gridded_los_data_" + ftype + ".sqlite"

    # loop through the datetimes in stms
    for i in range(len(stms)):
        stm = stms[i]
        etm = etms[i]

	# loop through the rads
	# store the multiprocess
	procs = []
	for rad in rads:
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

def convert_geo_to_mlt(rads, stms, etms, ftype = "fitacf",
	               stay_in_geo=False,
		       t_c_alt = 300.,
		       db_name=None,
		       dbdir="../data/sqlite3/",
		       run_in_parallel=False):

    """ converts latc and lonc from GEO to MLAT-MLT coords (MLT is in degrees).
    Also calcuates the azmimuthal velocity angle (in degrees) relative to the magnetic pole.
    NOTE : if stay_in_geo is set to False then origional latc, lonc do not change but 
    local time, ltc, in degrees (e.g. 0 (or 360) degree is midnight, 
    180 degrees is noon time) will be added."""

    from geo_to_mlt import worker
	
    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/geo_to_mlt.log",
                        level=logging.INFO)

    # loop through the datetimes in stms
    for i in range(len(stms)):
        stm = stms[i]
        etm = etms[i]

	# loop through the dates
	# store the multiprocess
	procs = []
	for rad in rads:
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

def bin_to_grids(rads, stms, etms, hemi="north",
		 ftype = "fitacf", coords="mlt",
		 db_name=None, dbdir="../data/sqlite3/",
                 run_in_parallel=False):

    """ bins the data into mlat-mlt-azm.
    NOTE: Set coords to "geo" if you want to remain in "geo" coords
    """

    from bin_data import worker

    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/bin_data.log",
                        level=logging.INFO)

    # loop through the datetimes in stms
    for i in range(len(stms)):
        stm = stms[i]
        etm = etms[i]

	# loop through the dates
	# store the multiprocess
	procs = []
	for rad in rads:
	    if run_in_parallel:
		# cteate a process
		worker_kwargs = {"stm":stm, "etm":etm, "ftype":ftype,
				 "coords":coords, "hemi":hemi,
				 "dbdir":dbdir, "db_name":db_name}
		p = mp.Process(target=worker, args=(rad),
			       kwargs=worker_kwargs)
		procs.append(p)

		# run the process
		p.start()
	    else:
		worker(rad, stm=stm, etm=etm, ftype=ftype,
		       coords=coords, hemi=hemi,
		       dbdir=dbdir, db_name=db_name)

	if run_in_parallel:
	    # make sure the processes terminate
	    for p in procs:
		p.join()

def median_filter(rads, stms, etms, hemi="north",
		  ftype = "fitacf", coords="mlt",
		  filtered_interval=2.,
		  input_dbname=None, output_dbname=None,
		  dbdir="../data/sqlite3/",
                  run_in_parallel=False):
    """ Bins the gridded data from a single radar into xxx-minute intervals.
    e.g., at each xxx-minute interval, median vector in each azimuth bin within a grid cell is
    selected as the representative velocity for that bin. 
    The results are stored in a different db such that data from a given radar
    are written into a single table named by the radar name.
    Then it runs combine_xxx_min_median to combine all the table into one"""

    from xxx_min_median import worker
    from combine_xxx_min_median import combine_xxx_min_median

    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/xxx_min_median.log",
                        level=logging.INFO)

    # loop through the datetimes in stms
    for i in range(len(stms)):
        stm = stms[i]
        etm = etms[i]

	# store the multiprocess
	procs = []
	# loop through radars
	for rad in rads:
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

    # take xxx minutes median values
    print("moving xxx_min_median of " + str(rads) + " into all_radars table")
    t1 = dt.datetime.now()
    combine_xxx_min_median(rads, ftype=ftype, coords=coords,
                           filtered_interval=filtered_interval,
                           dbdir=dbdir, db_name=output_dbname)
    t2 = dt.datetime.now()
    print("Finished moving. It took " +\
	  str((t2-t1).total_seconds() / 60.) + " mins\n")


    return

def create_master_db(ftype = "fitacf", coords="mlt",
		     filtered_interval=2.,
		     input_dbname=None, output_dbname=None,
		     dbdir="../data/sqlite3/",
		     master_table=True):

    from build_master_db import build_master_table 

    if input_dbname is None:
	input_dbname = "sd_" + str(int(filtered_interval)) + "_min_median_" +\
		       coords + "_" + ftype + ".sqlite"
    if output_dbname is None:
	output_dbname = "sd_master_" + coords + "_" + ftype + ".sqlite"

    input_table_1 = "all_radars"
    output_table_1 = "master_all_radars"
    input_table_2 = "master_all_radars"
    output_table_2 = "master_summary_all_radars"

    # create a log file to which any error occured will be written.
    logging.basicConfig(filename="./log_files/master_table_" + ".log",
                        level=logging.INFO)

    if master_table:
        # build a master table 
        print "building a master table"
        build_master_table(input_table_1, output_table_1,
                           ftype=ftype, coords=coords,
                           dbdir=dbdir, input_dbname=input_dbname,
                           output_dbname=output_dbname)
        print "A master table has been built"
    return

if __name__ == "__main__":

    # initialize parameters
    stms = [dt.datetime(2014, 12, 16, 13, 30)]
    etms = [dt.datetime(2014, 12, 16, 15, 0)]

    # NOTE: Do not forget to set the channel
    rads = ["wal", "bks", "fhe", "fhw", "cve", "cvw", "ade", "adw"]
    channels = [None, None, None, None, None, None, 'all', 'all']

    ftype = "fitacf"
    dbdir = "../data/sqlite3/"
    run_in_parallel = False

    do_move_to_db = False
    do_add_geolatc_geolonc = False
    do_convert_geo_to_mlt = False
    do_bin_to_grids = False 
    do_median_filter = False
    do_create_master_db = True

    db_name =  "sd_gridded_los_data_" + ftype + ".sqlite"

    # Move data from files to db 
    if do_move_to_db:
	move_to_db(stms, etms, rads, channels, db_name, ftype=ftype,
		   dbdir=dbdir, run_in_parallel=run_in_parallel)

    # Add geolatc geolonc to db
    if do_add_geolatc_geolonc:
	add_geolatc_geolonc(rads, stms, etms, ftype=ftype,
			    db_name=db_name, dbdir=dbdir,
			    run_in_parallel=run_in_parallel)
	
    # Convert GEO to MLT
    if do_convert_geo_to_mlt:
	convert_geo_to_mlt(rads, stms, etms, ftype=ftype,
			   stay_in_geo=False, t_c_alt = 300.,
			   db_name=db_name, dbdir=dbdir,
			   run_in_parallel=run_in_parallel)

    coords = "mlt"
    filtered_interval=2.
    input_dbname=None
    output_dbname=None
    # Bin to MLAT-MLT-AZM grids
    if do_bin_to_grids:
        bin_to_grids(rads, stms, etms, hemi="north",
		     ftype=ftype, coords=coords,
		     db_name=db_name, dbdir=dbdir,
		     run_in_parallel=run_in_parallel)

    # Median filter the data in each MLAT-MLT-AZ bin
    # Also combine all the tables int one
    if do_median_filter:
	median_filter(rads, stms, etms, hemi="north",
		      ftype = ftype, coords=coords,
		      filtered_interval=filtered_interval,
		      input_dbname=input_dbname,
		      output_dbname=output_dbname,
		      dbdir=dbdir, run_in_parallel=run_in_parallel)

    # Creat a master table
    if do_create_master_db:
	create_master_db(ftype=ftype, coords=coords,
			 filtered_interval=filtered_interval,
			 input_dbname=input_dbname,
			 output_dbname=output_dbname,
			 dbdir=dbdir, master_table=True)

