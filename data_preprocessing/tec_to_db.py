def tec_to_db(stm, etm, inpDir = "/sd-data/med_filt_tec/",
              table_name=None, db_name=None, dbdir="../data/sqlite3/"):

    """Writes GPS TEC data to a sqlite db
    """

    import pandas as pd
    from funcs import convert_to_datetime
    import sqlite3

    if db_name is None:
        db_name = "med_filt_tec.sqlite"
    if table_name is None:
        table_name = "med_filt_tec"
    # Make a db connection
    conn = sqlite3.connect(dbdir + db_name)

    # Read the median filtered TEC data
    inpColList = [ "dateStr", "timeStr", "Mlat",\
                          "Mlon", "med_tec", "dlat", "dlon" ]
    if stm.strftime("%Y%m%d") == stm.strftime("%Y%m%d"):
        ctimes = [stm]
    else:
        ctimes = [stm, etm]

    for ctime in ctimes:
        inpFile = inpDir + "tec-medFilt-" + ctime.strftime("%Y%m%d") + ".txt"
        print("reading data for " + ctime.strftime("%m/%d/%Y"))
        medFiltTECDF = pd.read_csv(inpFile, delim_whitespace=True,
                                   header=None, names=inpColList)

        print("Adding a datetime column")
        medFiltTECDF["datetime"] = medFiltTECDF.apply(convert_to_datetime, axis=1) 

        # Drop unwanted columns
        print("Droping unwanted columns and renaming other columns")
        medFiltTECDF = medFiltTECDF.drop(labels=["dateStr", "timeStr", "dlat", "dlon"], axis=1)
        # Rename columns
        medFiltTECDF.columns = [x.lower() for x in medFiltTECDF.columns]

        # Find the time closed to time interval of interest
        print("Selecting for the interval of interest")
        df = medFiltTECDF.loc[(medFiltTECDF.datetime >= stm) &\
                              (medFiltTECDF.datetime < etm), :]
   
        # Write data to db
        print("Writing data to db")
        df.to_sql(table_name, conn, if_exists="append", index=False)
        print("Done!")

    # Close db connection
    conn.close()

    return

if __name__ == "__main__":

    import datetime as dt

    # initialize parameters
#    stms = [dt.datetime(2013, 2, 21, 3, 2)]
#    etms = [dt.datetime(2013, 2, 21, 5, 2)]

#################################################
    # Create event list
    import sys
    sys.path.append("../data/")
    from create_event_list import create_event_list
    half_interval_length = 75.    # minutes
    #IMF_turning = "northward"
    IMF_turning = "all"
    IMF_events = True
    df_turn = create_event_list(IMF_events=IMF_events, IMF_turning=IMF_turning)

    # Find the convection respond time
    turn_dtms = [x.to_pydatetime() for x in df_turn.datetime]
    lag_times = df_turn.lag_time.as_matrix()
    event_dtms = [turn_dtms[i] + dt.timedelta(seconds=60. * lag_times[i])\
                  for i in range(len(turn_dtms))]

    # Construct stm and etm
    stms = [x - dt.timedelta(seconds=60. * half_interval_length) for x in event_dtms]
    etms = [x + dt.timedelta(seconds=60. * half_interval_length) for x in event_dtms]
#################################################

    db_name = "med_filt_tec.sqlite"
    table_name = "med_filt_tec"
    dbdir="../data/sqlite3/"
    inpDir = "/sd-data/med_filt_tec/"

#    stms = [stms[0]]
#    etms = [etms[0]]
    # loop through the datetimes in stms
    for i in range(len(stms)):
        stm = stms[i]
        etm = etms[i]

        # Write tec data to db
        tec_to_db(stm, etm, inpDir=inpDir, table_name=table_name,
                  db_name=db_name, dbdir=dbdir)



