import numpy as np
import datetime as dt
import logging
import sqlite3

def cosfit_superposed_epoch(input_table, output_table, db_name=None,
                            dbdir="../data/sqlite3/", ftype="fitacf", coords="mlt",
                            reltime_list=[-20, 20], reltime_resolution=2,
                            mlt_width=1., fit_by_bmazm=False, fit_by_losvel_azm=True,
                            abs_azm_maxlim = 90., abs_losvel_maxlim=500.,
                            fitvel_bounds=(-1000., 1000.), 
                            unique_azm_count_minlim=3, weighting=None):
    
    """ Does cosine fitting to all the LOS data in each MLAT/MLT grid, 
    and stores the results in a different table named "master_cosfit_superposed". 

    Parameters
    ----------
    input_table : str
        A table name in db_name db
    output_table : str
        A table name in db_name db
    db_name : str, default to None
        Name of the master db
    ftype : str
        SuperDARN file type
    coords : str
        Coordinates in which the binning process took place.
        Default to "mlt, can be "geo" as well. 
    reltime_list : a list of int
        The start of the relative time, e.g., -30, -20, 20, -30, ...,
    reltime_resolution : int
        The time resolution (in minutes) of the fitted data
        e.g.,  for reltime=-30 and reltime_resolution=2, the relative time range
        for a cos-fitting procedure would be [-30, -29]
    mlt_width : float
        The width of MLT region within which cosine fitting will be performed.
        i.e., for a point at 1 MLT, the points within 1  +/- 0.5*mlt_width will be
        used to fit a cosine curve.
    fit_by_bmazm : bool (Default to False)
        If set to true, the cosfitting would be done on losvel-vs-magbmazm.
    fit_by_losvel_azm : bool (Default to True)
        If set to true, the cosfitting would be done on losvel-vs-losvelazm.
    abs_azm_maxlim : float
        The absolute value of the maximum los vel. azm. (or mag. bmazm) beyond which data will be discarded.
        have to be qualified for cosfitting. 
    fitvel_bounds : tuple
        Values to put bounds on estimated 2-D vels
    weighting : str (Default to None)
        Type of weighting used for curve fitting
        if set to None, all azimuthal bins are
        considered equal regardless of the nubmer of points
        each of them contains.

    Returns
    -------
    Nothing

    
    """

    # construct a db name
    if db_name is None:
        db_name = "sd_master_" + coords + "_" +ftype + ".sqlite"

    # make a db connection
    try:
        conn = sqlite3.connect(dbdir + db_name,
                               detect_types = sqlite3.PARSE_DECLTYPES)
        cur = conn.cursor()
    except Exception, e:
        logging.error(e, exc_info=True)

    # set output_table name
    if weighting is not None:
        output_table = output_table + "_" + weighting + "_weight"

    # create output_table
    if coords == "mlt":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel_mag float(9,2)," +\
                  " vel_mag_err float(9,2)," +\
                  " vel_dir float(9,2)," +\
                  " vel_dir_err float(9,2)," +\
                  " vel_count INT," +\
                  " mag_gazmc_span REAL," +\
                  " mag_glatc float(7,2)," +\
                  " mag_gltc float(8,2)," +\
                  " relative_time SMALLINT, " +\
                  " CONSTRAINT reltime PRIMARY KEY (" +\
                  "mag_glatc, mag_gltc, relative_time))"
    elif coords == "geo":
        command = "CREATE TABLE IF NOT EXISTS {tb}" +\
                  "(vel_mag float(9,2)," +\
                  " vel_mag_err float(9,2)," +\
                  " vel_dir float(9,2)," +\
                  " vel_dir_err float(9,2)," +\
                  " vel_count INT," +\
                  " geo_gazmc_span REAL," +\
                  " geo_glatc float(7,2)," +\
                  " geo_gltc float(8,2)," +\
                  " relative_time SMALLINT, " +\
                  " CONSTRAINT reltime PRIMARY KEY (" +\
                  "geo_glatc, geo_gltc, relative_time))"
    command = command.format(tb=output_table)
    try:
        cur.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)

    # add new columns
    if coords == "mlt":
        col_glatc = "mag_glatc"   # glatc -> gridded latitude center
        col_gltc = "mag_gltc"     # mlt hour in degrees
        col_gazmc = "mag_gazmc"   # gazmc -> gridded azimuthal center
        col_gazmc_count = "mag_gazmc_count"
        col_azmc_span = "mag_gazmc_span"
    if coords == "geo":
        col_glatc = "geo_glatc"
        col_gltc = "geo_gltc"    # local time in degrees
        col_gazmc = "geo_gazmc"
        col_azmc_span = "geo_gazmc_span"

    try:
        cur.execute(command)
    except Exception, e:
        logging.error(e, exc_info=True)

    # Do the fitting for each range of relative time with a given relative time resolution
    for reltm in reltime_list:
        sreltm = reltm
        ereltm = reltm + (reltime_resolution-1)
        # query  the data
        command = "SELECT {glatc}, {gltc} FROM {tb2} "+\
                  "WHERE relative_time BETWEEN {sreltm} AND {ereltm} " +\
                  "GROUP BY {glatc}, {gltc}"
        command = command.format(tb2=input_table, glatc=col_glatc, gltc=col_gltc,
                                 sreltm=sreltm, ereltm=ereltm)
        try:
            cur.execute(command)
        except Exception, e:
            logging.error(e, exc_info=True)
        rws = cur.fetchall()

        lats = [x[0] for x in rws]
        lons = [x[1] for x in rws]

        # Do cosing fitting for each MLAT-MLT cell
        for ii in xrange(len(lats)):
            gltc_left = lons[ii] - mlt_width/2.*15.
            gltc_right = lons[ii] + mlt_width/2.*15.
            if gltc_right >=360 or gltc_left < 0:
                gltc_left =  gltc_left % 360
                where_clause = "AND (({gltc} BETWEEN {gltc_left} AND 360) or ({gltc} BETWEEN 0 AND {gltc_right})) "
            else:
                where_clause = "AND ({gltc} BETWEEN {gltc_left} AND {gltc_right}) "
            command = "SELECT vel, {gazmc} FROM {tb2} " +\
                      "WHERE {glatc}={lat} " + where_clause + \
                      "AND (relative_time BETWEEN {sreltm} AND {ereltm}) " +\
                      "AND ABS(vel) <= {abs_losvel_maxlim} "+\
                      "ORDER BY {gazmc}"
            command = command.format(tb2=input_table, glatc=col_glatc, gltc=col_gltc,
                                     gltc_left=gltc_left, gltc_right=gltc_right, 
                                     sreltm=sreltm, ereltm=ereltm,
                                     abs_losvel_maxlim=abs_losvel_maxlim,
                                     gazmc=col_gazmc, lat=lats[ii])
            try:
                cur.execute(command)
            except Exception, e:
                logging.error(e, exc_info=True)

            rows = cur.fetchall()
            if rows:
                los_vel = np.array([x[0] for x in rows])
                azm = np.array([x[1] for x in rows])

                # Limit azm range within +/- abs_azm_maxlim
                azm = [x if x <=180 else x-360 for x in azm]
                los_vel = [los_vel[i] for i in range(len(los_vel))\
                           if abs(azm[i]) <= abs_azm_maxlim]
                azm = [x for x in azm if abs(x) <= abs_azm_maxlim]
                vel_count = len(los_vel)
                unique_azm_count = len(np.unique(azm))
                if unique_azm_count <= unique_azm_count_minlim:
                    continue
                azm_span = np.max(azm) - np.min(azm)

                if weighting == "std":
                    los_vel_tmp = np.array(los_vel)
                    #std_val_tmp = np.std(los_vel_tmp[np.where(azm==azm[i])])
                    # Calculate std values of losvels in each azm bin.
                    # Add 1. to avoid having 0. values
                    sigma =  np.array([1. + np.std(los_vel_tmp[np.where(azm==x)]) for x in azm])
                else:
                    sigma =  np.array([1.0 for x in azm])

                # do cosine fitting with weight
                try:
                    fitpars, perrs = cos_curve_fit(azm, los_vel, sigma, bounds=fitvel_bounds)
                except:
                    continue
                vel_mag = round(fitpars[0],2)
                vel_dir = round(np.rad2deg(fitpars[1]) % 360,1)
                vel_mag_err = round(perrs[0],2)
                vel_dir_err = round(np.rad2deg(perrs[1]) % 360, 1)

                # populate the out table 
                command = "INSERT OR IGNORE INTO {tb1} (vel_mag, "+\
                          "vel_mag_err, vel_dir, vel_dir_err, vel_count, {azmc_span_txt}, "+\
                          "{glatc_txt}, {gltc_txt}, relative_time) VALUES ({vel_mag}, "\
                          "{vel_mag_err}, {vel_dir}, {vel_dir_err}, {vel_count}, "+\
                          "{azmc_span}, {glatc}, {gltc}, {reltime})"
                command = command.format(tb1=output_table, azmc_span_txt=col_azmc_span,
                                         glatc_txt=col_glatc,
                                         gltc_txt=col_gltc, vel_mag=vel_mag,
                                         vel_mag_err=vel_mag_err, vel_dir=vel_dir,
                                         vel_dir_err=vel_dir_err, vel_count=vel_count,
                                         azmc_span =azm_span, glatc=lats[ii], gltc=lons[ii],
                                         reltime=reltm)
                try:
                    cur.execute(command)
                except Exception, e:
                    logging.error(e, exc_info=True)
                print("finish inserting cosfit result at " +\
                      str((lats[ii], lons[ii], reltm)))

        try:
            conn.commit()
        except Exception, e:
            logging.error(e, exc_info=True)

    # close db connection
    conn.close()

    return

def cosfunc(x, Amp, phi):
    import numpy as np
    return Amp * np.cos(1 * x - phi)

def cos_curve_fit(azms, vels, sigma, bounds=(-np.inf, np.inf)):
    import numpy as np
    from scipy.optimize import curve_fit
    fitpars, covmat = curve_fit(cosfunc, np.deg2rad(azms), vels, sigma=sigma, bounds=bounds)
    perrs = np.sqrt(np.diag(covmat)) 

    return fitpars, perrs

def main():
    
    import logging

    # initialize parameters
    start_reltime = -30
    end_reltime = 30
    reltime_resolution = 2
    #reltime_list = range(start_reltime, end_reltime+reltime_resolution, reltime_resolution)
    #reltime_list = [-30, -20, -10, -6, 0, 6, 10, 20, 30]
    reltime_list = [-20, 20]
    mlt_width = 1.
    fit_by_bmazm=False   # Not implemented yet
    fit_by_losvel_azm = True
    abs_azm_maxlim = 75
    abs_losvel_maxlim=300
    unique_azm_count_minlim=3 
    fitvel_bounds=(-300., 300.)
    weighting = "std"
    #weighting = None     # No weighting is used

    input_table = "master_superposed_epoch"
    output_table = "master_cosfit_superposed_mltwidth_" + str(int(mlt_width)) +\
                   "_res_" + str(reltime_resolution) + "min"

    # create a log file to which any error occured between client and
    # MySQL server communication will be written.
    logging.basicConfig(filename="./log_files/master_cosfit_superposed.log",
                        level=logging.INFO)

    cosfit_superposed_epoch(input_table, output_table, db_name=None,
                            dbdir="../data/sqlite3/", ftype="fitacf", coords="mlt",
                            reltime_list=reltime_list, reltime_resolution=reltime_resolution,
                            mlt_width=mlt_width,
                            fit_by_bmazm=fit_by_bmazm, fit_by_losvel_azm=fit_by_losvel_azm,
                            abs_azm_maxlim=abs_azm_maxlim, abs_losvel_maxlim=abs_losvel_maxlim,
                            unique_azm_count_minlim=unique_azm_count_minlim, 
                            fitvel_bounds=fitvel_bounds,
                            weighting=weighting)


    return

if __name__ == "__main__":
    main()
