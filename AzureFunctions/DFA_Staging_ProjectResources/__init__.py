# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import requests
import json
import datetime
from xml.etree import ElementTree
import logging
from common import get_relative_blob_path, create_file_name, init_BA_session, close_BA_session, upload_blob_to_storage, Blue_Ant_rest_api_request, get_BA_SOAP_headers
import traceback
import os
from azure.storage.blob import BlobServiceClient
import pytz
import json
import config_urls as URL


def main(name: str) -> str:

    try:

        CONTAINER = 'blueantprojectresource'
        
        sessionID = init_BA_session()

        # ----------------- API CALL --------------------

        datenow = datetime.datetime.now().date()

        """
        response = Blue_Ant_rest_api_request('v1/human/persons', 'GET')

        BlueAntUserJsonData = json.loads(response.content)

        activeUserID = []

        for user in BlueAntUserJsonData["persons"]:
            if "leaveDate" not in user:
                activeUserID.append(user["id"])

        logging.info("activeUser in Liste gespeichert")
        """

        projectUrl = URL.BLUEANT_REST_PROJECTS
        response2 = Blue_Ant_rest_api_request(projectUrl, 'GET')

        BlueAntUserProjectData = json.loads(response2.content)

        activeProjectList = []

        for project in BlueAntUserProjectData["projects"]:
            projectEndDate = datetime.datetime.strptime(project["end"], '%Y-%m-%d').date()
            if datenow < projectEndDate:
                activeProjectList.append(project["id"])

        logging.info("active Project in Liste gespeichert")

        BlueAntheaders = get_BA_SOAP_headers()

        ###sessionid
        root = ElementTree.Element('root')

        # Namensraum
        ns = {'ns9': 'http://project.blueant.axis.proventis.net/'}


        for project in activeProjectList:
            BlueAntSOAPProjecturl = URL.BLUEANT_PROJECTSERVICE

            BlueAntSoappayload = "<soapenv:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:project=\"http://project.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n <project:getProjectResourcesRequestParameter>\r\n<project:sessionID>{}</project:sessionID>   \r\n <project:projectID>{}</project:projectID>\r\n<project:releaseType>all</project:releaseType>   \r\n <project:personID>{}</project:personID> \r\n </project:getProjectResourcesRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>".format(sessionID,project,name)

            BlueAntProjectResourceResponse = requests.request("POST", BlueAntSOAPProjecturl, headers=BlueAntheaders, data=BlueAntSoappayload)

            if BlueAntProjectResourceResponse.status_code != 200:

                logging.error("The response returned the wrong status code")

                raise Exception("The response returned the wrong status code")

            tree = ElementTree.fromstring(BlueAntProjectResourceResponse.content)
            
            #wir müssen die ProjectID manuell hinzufügen da diese in der Response fehlen.
            #Hier definieren wir das Element und den Wert. 
            project_id_element = ElementTree.Element('ns1:projectid')

            project_id_element.text = str(project)
            
            if tree.find('.//{http://project.blueant.axis.proventis.net/}resourceID') is not None:
                #Hier fügen wir die ProjectID dem XML zu.
                project_resource_element = tree.find('.//ns9:ProjectResource', namespaces=ns)
                project_resource_element.append(project_id_element)
                root.extend(tree)
        
        new_xml_data = ElementTree.tostring(root)

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")
    
     # ----------------- API CALL --------------------

        upload_blob_to_storage(f'{CONTAINER}/merge', new_xml_data, str(name)+'.xml')

        close_BA_session(sessionID)

        return 1
  
    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()
        logging.info(error_traceback)
        return 0
