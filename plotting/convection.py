import matplotlib
matplotlib.use('Agg')
import sqlite3
import datetime as dt
import numpy as np 
import logging
import sys
sys.path.append("../data/")
from build_event_database import build_event_database

def fetch_data(input_table, lat_range=[52, 59], nvel_min=3, relative_time=-30,
               reltime_resolution=2, mlt_width=1.,
               mlt_range=[-6, 6], gazmc_span_minlim=10, 
               vel_mag_maxlim=500., vel_mag_err_maxlim=0.2,
               fit_by_bmazm=False, fit_by_losvel_azm=True,
               ftype="fitacf", coords="mlt",
               dbdir="../data/sqlite3/", db_name=None, weighting=None):

    """ fetch fitted data from the master db into a dict

    Parameters
    ----------
    input_table : str
        A table name in db_name db
    lat_ragne : list
	The range of latitudes of interest
    nvel_min : int
        minimum requirement for the number of velocity measurements
	in a lat-lon grid cell
    relative_time : int
        The relative time of interest
    reltime_resolution : int
        The time resolution (in minutes) of the fitted data
        e.g.,  for start_reltime=-30 and reltime_resolution=2, the relative time range
        for a cos-fitting procedure would be [-30, -29]
    mlt_range : list
        The MLT range of interest
    mlt_width : float
        The width of MLT region within which cosine fitting will be performed.
        i.e., for a point at 1 MLT, the points within 1  +/- 0.5*mlt_width will be
        used to fit a cosine curve.
    fit_by_bmazm : bool (Default to False)
        If set to true, the cosfitting would be done on losvel-vs-magbmazm.
    fit_by_losvel_azm : bool (Default to True)
        If set to true, the cosfitting would be done on losvel-vs-losvelazm.
    db_name : str, default to None
        Name of the master db
    ftype : str
        SuperDARN file type
    coords : str
        Coordinates in which the binning process took place.
        Default to "mlt, can be "geo" as well.
    weighting : str (Default to None)
        Type of weighting used for curve fitting
        if set to None, all azimuthal bins are
        considered equal regardless of the nubmer of points
        each of them contains.


    Return
    ------
    data_dict : dict

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

    # set input_table name
    if weighting is not None:
        input_table = input_table + "_" + weighting + "_weight"

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

    # select data from the master_cosfit table for the MLT region of interest
    gltc_left = (mlt_range[0] * 15.) % 360
    gltc_right = (mlt_range[1] * 15.) % 360
    if gltc_right < gltc_left:
        where_clause = "AND (({gltc} BETWEEN {gltc_left} AND 360) or ({gltc} BETWEEN 0 AND {gltc_right})) "
    else:
        where_clause = "AND ({gltc} BETWEEN {gltc_left} AND {gltc_right}) "
    command = "SELECT vel_count, vel_mag, vel_dir, {glatc}, {gltc}, " +\
              "vel_mag_err, vel_dir_err, relative_time, {azmc_span_txt}, (vel_mag_err/vel_mag) FROM {tb1} " +\
              "WHERE ({glatc} BETWEEN {lat_min} AND {lat_max}) " +\
              where_clause +\
              "AND {azmc_span_txt} >= {gazmc_span_minlim} "+\
              "AND vel_count >= {nvel_min} "+\
              "AND ABS(vel_mag_err/vel_mag) <= {vel_mag_err_maxlim} "+\
              "AND ABS(vel_mag) <= {vel_mag_maxlim} "+\
              "AND relative_time = {reltime}"
    command = command.format(tb1=input_table, glatc=col_glatc,
                             gltc = col_gltc, lat_min=lat_range[0],
                             lat_max=lat_range[1], nvel_min=nvel_min,
                             vel_mag_err_maxlim=vel_mag_err_maxlim,
                             vel_mag_maxlim=vel_mag_maxlim,
                             gltc_left=gltc_left, gltc_right=gltc_right,
                             azmc_span_txt=col_azmc_span, gazmc_span_minlim=gazmc_span_minlim,
                             reltime=relative_time)
    cur.execute(command)
    rws = cur.fetchall()

    data_dict = {}
    # filter the data based on lattitude range. 
    data_dict['vel_count'] = np.array([x[0] for x in rws])
    data_dict['vel_mag'] = np.array([x[1] for x in rws])
    data_dict['vel_dir'] = np.array([x[2] for x in rws])
    data_dict['glatc'] = np.array([x[3] for x in rws])
    data_dict['glonc'] = np.array([x[4] for x in rws])
    data_dict['vel_mag_err'] = np.array([x[5] for x in rws])
    data_dict['vel_dir_err'] = np.array([x[6] for x in rws])
    data_dict['relative_time'] = np.array([x[7] for x in rws])
    data_dict['gazmc_span'] = np.array([x[8] for x in rws])
    data_dict['vel_mag_err_ratio'] = np.array([x[9] for x in rws])

    # close db connection
    conn.close()

    return data_dict

def cart2pol(x, y):
    import numpy as np
    rho = np.sqrt(x**2 + y**2)
    phi = np.arctan2(y, x)
    return (phi, rho)

def pol2cart(phi, rho):
    import numpy as np
    x = rho * np.cos(phi)
    y = rho * np.sin(phi)
    return(x, y)

def vector_plot(ax, data_dict, cmap=None, norm=None, velscl=1, lat_min=50, title="xxx",
                hemi="north", fake_pole=False):
    
    """ plots the flow vectors in LAT/LT grids in coords

    Parameters
    ----------
    
    """

    import matplotlib.pyplot as plt
    import matplotlib as mpl
    from matplotlib.collections import PolyCollection,LineCollection
    import numpy as np

    # set axis limits
    if fake_pole:
        fake_pole_lat = 70
    else:
        fake_pole_lat = 90

    rmax = fake_pole_lat - lat_min
    ax.set_xlim([-rmax, rmax])
    ax.set_ylim([-rmax, 0])
    ax.set_aspect("equal")

    # remove tikcs
    ax.tick_params(axis='both', which='both', bottom='off', top='off',
                   left="off", right="off", labelbottom='off', labelleft='off')

    # plot the latitudinal circles
    for r in [10, 30, 50]:
        c = plt.Circle((0, 0), radius=r, fill=False, linewidth=0.5)
        ax.add_patch(c)

    # plot the longitudinal lines
    for l in np.deg2rad(np.array([210, 240, 270, 300, 330])):
        x1, y1 = pol2cart(l, 10)
        x2, y2 = pol2cart(l, 50)
        ax.plot([x1, x2], [y1, y2], 'k', linewidth=0.5)


    x1, y1 = pol2cart(np.deg2rad(data_dict['glonc']-90),
                      fake_pole_lat-np.abs(data_dict['glatc']))

    # add the vector lines
    lines = []
    intensities = []
    vel_mag = data_dict['vel_mag']

    # calculate the angle of the vectors in a tipical x-y axis.
    theta = np.deg2rad(data_dict['glonc'] + 90 - data_dict['vel_dir']) 

    # make the points sparse
    sparse_factor = 2
    x1 = np.array([x1[i] for i in range(len(x1)) if i%sparse_factor==0])
    y1 = np.array([y1[i] for i in range(len(y1)) if i%sparse_factor==0])
    vel_mag = np.array([vel_mag[i] for i in range(len(vel_mag)) if i%sparse_factor==0])
    theta = np.array([theta[i] for i in range(len(theta)) if i%sparse_factor==0])


    x2 = x1+vel_mag/velscl*(-1.0)*np.cos(theta)
    y2 = y1+vel_mag/velscl*(-1.0)*np.sin(theta)
    lines.extend(zip(zip(x1,y1),zip(x2,y2)))

    #save the param to use as a color scale
    intensities.extend(np.abs(vel_mag))

    # plot the velocity locations
    ccoll = ax.scatter(x1, y1,
                        s=1.0,zorder=10,marker='o', c=np.abs(np.array(intensities)),
                        linewidths=.5, edgecolors='face'
                        ,cmap=cmap,norm=norm)
    lcoll = LineCollection(np.array(lines),linewidths=0.5,zorder=12
                        ,cmap=cmap,norm=norm)
    lcoll.set_array(np.abs(np.array(intensities)))
    ccoll.set_array(np.abs(np.array(intensities)))
    ax.add_collection(ccoll)
    ax.add_collection(lcoll)

    # add text
    ax.set_title(title, fontsize=8)
    # add latitudinal labels
    fnts = 'small'
    if hemi=="north":
        ax.annotate("80", xy=(0, -10), ha="left", va="bottom", fontsize=fnts)
        ax.annotate("60", xy=(0, -30), ha="left", va="bottom", fontsize=fnts)
    elif hemi=="south":
        ax.annotate("-80", xy=(0, -10), ha="left", va="bottom", fontsize=fnts)
        ax.annotate("-60", xy=(0, -30), ha="left", va="bottom", fontsize=fnts)

    # add mlt labels
    ax.annotate("0", xy=(0, -rmax), xytext=(0, -rmax-1), ha="center", va="top", fontsize=fnts)
    ax.annotate("6", xy=(rmax, 0), xytext=(rmax+1, 0), ha="left", va="center", fontsize=fnts)
    ax.annotate("18", xy=(-rmax, 0), xytext=(-rmax-1, 0), ha="right", va="center", fontsize=fnts)

    return lcoll

def add_cbar(fig, coll, bounds=None, label="Velocity [m/s]",
             shrink=0.65, cax=None):

    # add color bar
    if cax:
        cbar=fig.colorbar(coll, cax=cax, orientation="vertical",
                          boundaries=bounds, drawedges=False) 
    else:
        cbar=fig.colorbar(coll, orientation="vertical", shrink=shrink,
                          boundaries=bounds, drawedges=False) 

    #define the colorbar labels
    if bounds:
        l = []
        for i in range(0,len(bounds)):
            if i == 0 or i == len(bounds)-1:
                l.append(' ')
                continue
            l.append(str(int(bounds[i])))
        cbar.ax.set_yticklabels(l)
        #cbar.ax.tick_params(axis='y',direction='out')
    cbar.set_label(label)

    return


def main():

    import datetime as dt
    import matplotlib.pyplot as plt
    import matplotlib as mpl

    # input parameters
    nvel_min=20
    lat_range=[54, 60]
    lat_min = 50
    reltime_resolution=2
    mlt_width=2.
    mlt_range=[-6, 6]
    gazmc_span_minlim=30
    vel_mag_err_maxlim=0.2
    vel_mag_maxlim=200.
    fit_by_bmazm=False     # Not implemented yet
    fit_by_losvel_azm=True
    ftype = "fitacf"
    coords = "mlt"
    #weighting = None 
    weighting = "std"
    dbdir="../data/sqlite3/"
    #IMF_turning = "southward"
    IMF_turning = "northward"

    input_table = "master_cosfit_superposed_" + IMF_turning + "_mltwidth_" + str(int(mlt_width)) +\
                   "_res_" + str(reltime_resolution) + "min"

    # cmap and bounds for color bar
    color_list = ['purple', 'b', 'c', 'g', 'y', 'r']
    cmap = mpl.colors.ListedColormap(color_list)
    bounds = range(0, 180, 30)
    bounds.append(10000)
    # build a custom color map
    norm = mpl.colors.BoundaryNorm(bounds, cmap.N)


    #relative_times = [-30, -20, -10, -6, 0, 6, 10, 20, 30]
    relative_times = [-20, -10, -6, 0, 10, 20]

    #fig_dir = "../plots/convection/"
    fig_dir = "/home/muhammad/Dropbox/tmp/convection/"
    fig_name = "3_by_2_superposed_convection_" + IMF_turning + "_lat" + str(lat_range[0]) +\
               "_to_lat" + str(lat_range[1]) +\
               "_mltwidth_" + str(int(mlt_width)) +\
               "_res_" + str(reltime_resolution) + "min" + \
               "_nvel_min_" + str(nvel_min) + "velmag_err_"  + str(vel_mag_err_maxlim)
   
    # create subplots
    #fig, axes = plt.subplots(nrows=len(relative_times)/3, ncols=3, figsize=(8,6))
    fig, axes = plt.subplots(nrows=len(relative_times)/3, ncols=3, figsize=(8,4))
    fig.subplots_adjust(hspace=-0.2)
    #fig.subplots_adjust(hspace=0.3)


    if len(relative_times) == 1:
        axes = [axes]
    else:
        axes = axes.flatten().tolist()

    for i, relative_time in enumerate(relative_times):
        # fetches the data from db 
        data_dict = fetch_data(input_table, lat_range=lat_range, nvel_min=nvel_min,
                               relative_time=relative_time, reltime_resolution=reltime_resolution,
                               mlt_width=mlt_width, mlt_range=mlt_range, gazmc_span_minlim=gazmc_span_minlim,
                               fit_by_bmazm=fit_by_bmazm, fit_by_losvel_azm=fit_by_losvel_azm,
                               vel_mag_maxlim=vel_mag_maxlim,
                               vel_mag_err_maxlim=vel_mag_err_maxlim,
                               ftype=ftype, coords=coords, dbdir=dbdir,
                               db_name=None, weighting=weighting)

        # plot the flow vectors
        title = "Time = " + str(relative_time)
        coll = vector_plot(axes[i], data_dict, cmap=cmap, norm=norm, velscl=10,
                           lat_min=lat_min, title=title)

    # add colorbar
    fig.subplots_adjust(right=0.85)
    cbar_ax = fig.add_axes([0.90, 0.25, 0.02, 0.5])
    add_cbar(fig, coll, bounds, cax=cbar_ax, label="Speed [m/s]")

    # Set a figure title
    df_turn = build_event_database(IMF_turning=IMF_turning, event_status="good")
    nevents = len(df_turn.datetime.unique())
    fig_title = str(nevents) + " IMF " + IMF_turning.capitalize() + " Turning Events"
    fig.suptitle(fig_title, y=0.90, fontsize=15)

    # save the fig
    fig.savefig(fig_dir + fig_name + ".png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    #plt.show()

    return

if __name__ == "__main__":
    main()
