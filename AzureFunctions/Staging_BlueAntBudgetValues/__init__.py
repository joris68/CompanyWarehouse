import requests
import json
import datetime
from xml.etree import ElementTree
import logging
from common import get_BA_SOAP_headers, init_BA_session, close_BA_session, get_relative_blob_path, create_file_name, Blue_Ant_rest_api_request, upload_blob_to_storage
import traceback
import config_urls as URL

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
        
        CONTAINER = 'blueantbudgetvalues'

        sessionID = init_BA_session()

    #----------------- API Call ---------------------------------------
        datenow = datetime.datetime.now().date()

        RESTBlueAntProjectResponse = Blue_Ant_rest_api_request(URL.BLUEANT_REST_PROJECTS, 'GET')

        BlueAntUserProjectData = json.loads(RESTBlueAntProjectResponse.text)

        activeProjectList = []

        for project in BlueAntUserProjectData["projects"]:
            projectEndDate = datetime.datetime.strptime(project["end"], '%Y-%m-%d').date()
            if datenow < projectEndDate:
                activeProjectList.append(project["id"])

        logging.info("active Project in Liste gespeichert")

        BlueAntheaders = get_BA_SOAP_headers()

        root = ElementTree.Element('root')

        for project in activeProjectList:

            BlueAntBudgetValuespayload = "<soapenv:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:budget=\"http://budget.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n <budget:getBudgetValuesRequestParameter>\r\n<budget:sessionID>{}</budget:sessionID>\r\n<budget:projectID>{}</budget:projectID>\r\n<budget:dateFrom xsi:nil=\"true\"/>\r\n<budget:dateTo xsi:nil=\"true\"/>       \r\n </budget:getBudgetValuesRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>".format(sessionID,project)

            BlueAntProjectRolesresponse = requests.request("POST", URL.BLUEANT_BUDGETSERVICE, headers=BlueAntheaders, data=BlueAntBudgetValuespayload)

            if BlueAntProjectRolesresponse.status_code != 200:
                logging.error("The response returned the wrong status code")

                raise Exception("The response returned the wrong status code")

            tree = ElementTree.fromstring(BlueAntProjectRolesresponse.content)
            
            root.extend(tree)

        new_xml_data = ElementTree.tostring(root)

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")

    #----------------- API Call ---------------------------------------

        upload_blob_to_storage(f'{CONTAINER}/'+ get_relative_blob_path(), new_xml_data, create_file_name('.xml') )

        logging.info("Blob succesfully loaded into blob storage")

        close_BA_session(sessionID)

        return func.HttpResponse("Fucntion was successfully executed", status_code=200)
    
    except Exception as e:

         # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)

