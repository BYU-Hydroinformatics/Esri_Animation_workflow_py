################################################################
#
# File: update_features.py
# Author: Michael Souffront
# Date: 05/22/2018
# Last Modified: 09/12/2018
# Purpose: Update published service with new dissolved FTs
# Requirements: arcpy
#
################################################################

# import modules
import arcpy
import os
from ALFlib.ALFlib import copyFiles
import logging


# create logger function
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler(r'C:\Users\byuhi\Documents\table_update_workflow\workflow.log', 'a', 'utf-16')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == "__main__":
    # initialize logger
    init_logger()
    logging.debug('Updating features')

    # set arcpy env variables
    arcpy.gp.logHistory = False
    arcpy.env.overwriteOutput = True

    source = r'D:\dissolved_features.gdb'
    destination = r'C:\Users\byuhi\Documents\ArcGIS\animation_workspace'

    # compact gdb
    arcpy.Compact_management(source)

    # delete old gdb
    arcpy.Delete_management(os.path.join(destination, 'dissolved_features.gdb'))

    # copy gdb to production
    copyFiles(source, destination, exclusions=['*.lock'], overwrite=True)

    logging.debug('Finished updating features')
