######################################################################################
#
# File: update_services.py
# Author: Michael Souffront
# Date: 09/13/2018
# Last Modified: 09/13/2018
# Purpose: Update published services by stopping and them and then restarting them
# Requirements:
# Modified from: https://community.esri.com/thread/186020-start-stop-map-service-arcpy
#
######################################################################################

# import modules
import urllib
import urllib2
import json
import contextlib
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
def main(args):
    # initialize logger
    init_logger()

    # parse arguments
    action = args[0]  # "start" or "stop"
    server = args[1]
    token = args[2]
    service = args[3]

    try:
        logging.debug('Performing action: {0} for {1}'.format(action, service))
        json_output = service_start_stop(server, service, action, token)

        # validate json object result
        if json_output['status'] == "success":
            logging.debug('{0} {1} successful'.format(action.title(), service))
        else:
            logging.debug('Failed to {0} {1}'.format(action, service))
            raise Exception(json_output)

    except Exception, err:
        logging.error(err)


# function to generate a token from arcgis server; returns token
def get_token(admin_user, admin_pass, server, expiration):
    # create url
    url = "http://{0}/arcgis/admin/generateToken?f=json".format(server)

    # encode the query string
    query_dict = {
        'username': admin_user,
        'password': admin_pass,
        'expiration': str(expiration),
        'client': 'requestip'
    }
    query_string = urllib.urlencode(query_dict)

    try:
        # request the token
        with contextlib.closing(urllib2.urlopen(url, query_string)) as json_response:
            get_token_result = json.loads(json_response.read())
            # validate result
            if "token" not in get_token_result or get_token_result == None:
                logging.error("Failed to get token: {0}".format(get_token_result['messages']))
                raise Exception("Failed to get token: {0}".format(get_token_result['messages']))
            else:
                return get_token_result['token']

    except urllib2.URLError, e:
        logging.error("Could not connect to machine {0}\n{1}".format(server, e))
        raise Exception("Could not connect to machine {0}\n{1}".format(server, e))


# function to start or stop a service on arcgis server; returns json response.
def service_start_stop(server, service, action, token):
    # create URL
    url = "http://{0}/arcgis/admin".format(server)
    request_url = url + "/services/{0}/{1}".format(service, action)

    # encode the query string
    query_dict = {
        "token": token,
        "f": "json"
    }
    query_string = urllib.urlencode(query_dict)

    # send the server request and return the json response
    with contextlib.closing(urllib.urlopen(request_url, query_string)) as json_response:
        return json.loads(json_response.read())


# function to start or stop a service on arcgis server; returns json response.
def flush_service_cache(server, folder_name, token):
    # create URL
    request_url = "http://{0}/arcgis/admin/system/handlers/rest/cache/clear".format(server)

    # encode the query string
    query_dict = {
        "folderName": folder_name,
        "token": token,
        "f": "json"
    }
    query_string = urllib.urlencode(query_dict)

    # send the server request and return the json response
    with contextlib.closing(urllib.urlopen(request_url, query_string)) as json_response:
        logging.debug(json.loads(json_response.read()))


if __name__ == '__main__':
    # initialize logger
    init_logger()
    logging.debug('Updating published services')

    # variables
    admin_user = "siteadmin"
    admin_pass = "arcserver"
    server = "ai4e-arcserver.byu.edu"
    folder_name = "global"

    # services = ["global/south_asia.MapServer", "global/south_america.MapServer", "global/africa.MapServer",
    #             "global/north_america.MapServer"]

    expiration = 60  # token timeout in minutes

    # get arcgis server token
    logging.debug('Getting token')
    token = get_token(admin_user, admin_pass, server, expiration)

    flush_service_cache(server, folder_name, token)

    # stop_args = [["stop", server, token, srv] for srv in services]
    # start_args = [["start", server, token, srv] for srv in services]
    #
    # # create pool for multiprocessing
    # p = Pool()
    # # call main function with stop args
    # stop_result = p.map(main, stop_args)
    # # close pool and wait for it to finish
    # p.close()
    # p.join()
    #
    # p = Pool()
    # # call main function with start args
    # start_result = p.map(main, start_args)
    # # close pool and wait for it to finish
    # p.close()
    # p.join()

    logging.debug('Finished updating published services')
