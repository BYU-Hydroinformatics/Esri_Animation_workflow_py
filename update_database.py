################################################################
#
# File: update_database.py
# Author: Michael Souffront
# Date: 01/17/2018
# Last Modified: 09/12/2018
# Purpose: Update summary table in SQL DB with new forecasts
# Requirements: psycopg2
#
################################################################

# import modules
import psycopg2 as pg
import os
import logging
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
def update_database(file_name):
    # initialize logger
    init_logger()

    # connect to sql database
    conn = pg.connect('host=localhost dbname=entgdb user=sde password=AIforBYU')
    cur = conn.cursor()

    # parse region from csv file name
    global_region = file_name.split('summary_table_')[1].split('-')[0]
    logging.debug('Working on {0} forecast'.format(global_region))

    # drop old (backup) table
    cur.execute("""drop table if exists sde.%s_summary_table_old;""" % global_region)

    # check if summary table schema exists
    cur.execute(
        """select exists (
        select 1 
        from information_schema.tables 
        where table_schema = 'sde' 
        and table_name = '%s_summary_table');""" % global_region
    )

    exists = cur.fetchone()[0]

    # create table schema if table does not exist (e.g. first time run)
    if not exists:
        cur.execute(
            """create table sde.%s_summary_table (
            id integer, watershed text, subbasin text, comid integer,
            return2 double precision, return10 double precision,
            return20 double precision, index integer, timestamp timestamp,
            max double precision, mean double precision,
            min double precision, style text, flow_class integer,
            primary key (id));""" % global_region
        )


    # create new backup table
    cur.execute("""create table sde.%s_summary_table_old as table sde.south_asia_summary_table;""" % global_region)

    # truncate existing table in preparation to populate with new forecast
    cur.execute("""truncate table sde.%s_summary_table;""" % global_region)

    # read csv file and populate table from it
    with open(file_name, 'r') as f:
        table_name = '"%s_summary_table"' % global_region
        cur.copy_from(f, table_name, sep=',')

    # save changes to sql database
    conn.commit()
    conn.close()

    return 'update_database for {0} finished'.format(file_name)


if __name__ == "__main__":
    # initialize logger
    init_logger()
    logging.debug('Adding new forecast tables to sql database')

    # create list with csv file names from mounted drive
    file_list = []
    for base_name in os.listdir('Z:'):
        file_list.append(os.path.join('Z:\\', base_name))

    # create pool for multiprocessing
    p = Pool()
    # call main function
    result = p.map(update_database, file_list)
    # close pool and wait for it to finish
    p.close()
    p.join()

    logging.debug('Finished adding new forecast tables to sql database')
