import requests
import json
import datetime
from xml.etree import ElementTree
import logging
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, init_BA_session, close_BA_session, Blue_Ant_rest_api_request, get_BA_SOAP_headers
import traceback
import azure.functions as func
import config_urls as URL


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantprojectroles'
       
        sessionID = init_BA_session()

        #-------------API CALL ------------

        response = Blue_Ant_rest_api_request(URL.BLUEANT_REST_PROJECTS, 'GET')

        BlueAntUserProjectData = json.loads(response.content)

        datenow = datetime.datetime.now().date()

        activeProjectList = []

        for project in BlueAntUserProjectData["projects"]:
            projectEndDate = datetime.datetime.strptime(project["end"], '%Y-%m-%d').date()

            if datenow < projectEndDate:
                activeProjectList.append(project["id"])

        logging.info("active Project in Liste gespeichert")

        root = ElementTree.Element('root')

        BlueAntheaders = get_BA_SOAP_headers()

        for project in activeProjectList:
        
            BlueAntProjectpayload = " <soapenv:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:project=\"http://project.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n <project:getProjectRolesRequestParameter>\r\n<project:sessionID>{}</project:sessionID>   \r\n <project:projectID>{}</project:projectID>  \r\n</project:getProjectRolesRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>".format(sessionID,project)

            BlueAntProjectRolesresponse = requests.request("POST",URL.BLUEANT_PROJECTSERVICE, headers=BlueAntheaders, data=BlueAntProjectpayload)

            if BlueAntProjectRolesresponse.status_code != 200:

                logging.error("The response returned the wrong status code")

                raise Exception("The response returned the wrong status code")

            tree = ElementTree.fromstring(BlueAntProjectRolesresponse.content)
            root.extend(tree)
               
        new_xml_data = ElementTree.tostring(root)

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")

        #-------------API CALL ------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(),  new_xml_data, create_file_name('.xml') )

        close_BA_session(sessionID)

        return func.HttpResponse("Function was successfully executed", status_code=200)

    except Exception as e:

         # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
