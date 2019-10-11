################################################################
#
# File: dissolve_features.py
# Author: Michael Souffront
# Date: 03/05/2018
# Last Modified: 09/12/2018
# Purpose: Create dissolved FTs based on new forecast
# Requirements: arcpy, psycopg2
#
################################################################

# import modules
import arcpy
import psycopg2 as pg
import logging
from timeit import default_timer as timer
from multiprocessing import Pool

# create logger function
def init_logger():

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(r'C:\Users\byuhi\Documents\table_update_workflow\workflow.log', 'a', 'utf-16')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# main function
def dissolve_features(region_zoom_pair):
    # initialize logger
    init_logger()
    # set arcpy overwrite to true
    arcpy.env.overwriteOutput = True
    # set workspace
    arcpy.env.workspace = r'C:\Users\byuhi\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\Connection to 13.90.208.101.sde'

    # connect to sql database
    conn = pg.connect('host=localhost dbname=entgdb user=sde password=AIforBYU')

    # extract region and zoom level from arg input
    region = region_zoom_pair[0]
    zoom = region_zoom_pair[1]
    spatial_ix = {'large': 3000000, 'medium': 1000000}

    # dict with region long name
    regions_long_name = {'s_asia': 'south_asia', 's_america': 'south_america', 'africa': 'africa',
                         'australia': 'australia', 'n_america': 'north_america', 'c_america': 'central_america',
                         'comoros': 'comoros'}  # , 'asia': 'asia'}
    cur = conn.cursor()

    # get a list of features based on region and zoom level
    fc_list = arcpy.ListFeatureClasses('*{0}_{1}_mean'.format(region, zoom))

    # get timestamps from forecast table based on region
    cur.execute("""select distinct timestamp from %s_summary_table order by timestamp;""" % regions_long_name[region])
    dates = cur.fetchall()

    # create a list of temp dissolved layers based on timestamp/date and other common fields
    temp_layer_list = []
    try:
        for date in dates:
            logging.debug('Working on {0} - {1} date {2}'.format(regions_long_name[region], zoom,
                                                                 date[0].strftime('%Y-%m-%d %H:%M:%S')))

            # main query
            # Only query the attributes that are needed to execute the dissolve
            # query = "select objectid, comid, order_, watershed, subbasin, region, shape, return2, return10, return20, " \
            #         "index, timestamp, mean, style, flow_class from " + fc_list[0] + " where timestamp = timestamp '" + \
            #         date[0].strftime('%Y-%m-%d %H:%M:%S') + "'"
            query = "select objectid, order_, region, shape, " \
                    "timestamp, style, flow_class from " + fc_list[0] + " where timestamp = timestamp '" + \
                    date[0].strftime('%Y-%m-%d %H:%M:%S') + "'"

            # identify timestamp
            dissolve_timestamp = date[0].strftime('%Y-%m-%d %H:%M:%S')

            # create query layer
            arcpy.MakeQueryLayer_management(arcpy.env.workspace, '{0}_{1}_temp_layer'.format(region, date[0].strftime('%Y%m%d%H%M%S')), query)

            # dissolve the timestamp query layer based on a list of common field values (for optimal visualization)
            arcpy.Dissolve_management('{0}_{1}_temp_layer'.format(region, date[0].strftime('%Y%m%d%H%M%S')),
                                      'in_memory/{0}_{1}'.format(region, date[0].strftime('%Y%m%d%H%M%S')),
                                      # ['watershed', 'subbasin', 'region', 'order_', 'timestamp', 'style', 'flow_class'])
                                      ['timestamp', 'region', 'order_', 'style', 'flow_class'])

            # Add back in the timestamp. It is not taken in the dissolve to save query time.
            # arcpy.AddField_management('in_memory/{0}_{1}'.format(region, date[0].strftime('%Y%m%d%H%M%S')),
            #                           "timestamp2", "DATE")
            # arcpy.CalculateField_management('in_memory/{0}_{1}'.format(region, date[0].strftime('%Y%m%d%H%M%S')),
            #                                 "timestamp2", "'" + dissolve_timestamp + "'", "PYTHON")

            # add dissolved layer to list
            temp_layer_list.append('in_memory/{0}_{1}'.format(region, date[0].strftime('%Y%m%d%H%M%S')))

            # remove timestamp from dissolve, then add a field and then calculate field to add the timestamp back in ****

    except Exception as e:
        logging.debug(str(e))

    # check that temp gdb exists. Create it if not (reason for this is that D drive empties on restart)
    if not arcpy.Exists(r'D:\dissolved_features.gdb'):
        arcpy.CreateFileGDB_management (r'D:', 'dissolved_features')

    # get temp gdb path
    temp_dir = r'D:\dissolved_features.gdb'

    # saves dissolved layers to temp gdb
    arcpy.Merge_management(temp_layer_list, temp_dir + r'\{0}_{1}_dissolved_current'.format(region, zoom))

    # add spatial index
    arcpy.AddSpatialIndex_management(temp_dir + r'\{0}_{1}_dissolved_current'.format(region, zoom), spatial_ix[zoom])

    # clears in memory workspace
    #arcpy.Delete_management('in_memory')

    conn.close()

    arcpy.ClearWorkspaceCache_management(arcpy.env.workspace)
    arcpy.env.workspace = ''

    return 'dissolve_features for {0} finished'.format(regions_long_name[region])


if __name__ == "__main__":
    # initialize logger
    init_logger()
    logging.debug('Dissolving features based on forecast')

    # set arcpy env variables
    arcpy.gp.logHistory = False
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = r'C:\Users\byuhi\AppData\Roaming\ESRI\Desktop10.5\ArcCatalog\Connection to 13.90.208.101.sde'

    # clear connections before main operation
    arcpy.DisconnectUser(arcpy.env.workspace, "ALL")

    # list of regions to dissolve
    regions = ['s_asia', 's_america', 'africa', 'n_america', 'c_america', 'comoros', 'australia']
    # list of zoom levels to dissolve
    zoom_levels = ['medium', 'large']

    # combine regions and zoom level lists to pass as one argument in multiprocessing pool
    region_zoom_pairs = []
    for reg in regions:
        for lvl in zoom_levels:
            region_zoom_pairs.append([reg, lvl])

    # create pool for multiprocessing
    p = Pool()
    # call main function
    result = p.map(dissolve_features, region_zoom_pairs)
    # close pool and wait for it to finish
    p.close()
    p.join()

    # release hold on sde
    arcpy.ClearWorkspaceCache_management(arcpy.env.workspace)
    arcpy.env.workspace = ''

    logging.debug('Finished dissolving features')
