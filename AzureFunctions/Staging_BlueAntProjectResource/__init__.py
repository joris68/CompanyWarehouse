import json
import datetime
from xml.etree import ElementTree
import logging
from common import get_relative_blob_path, create_file_name, init_BA_session, close_BA_session, upload_blob_to_storage, Blue_Ant_rest_api_request, get_BA_SOAP_headers
from .common_async import fetch_project_resource
import asyncio
import traceback
import json
import config_urls as URL

import azure.functions as func


async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        CONTAINER = 'blueantprojectresource'
        datenow = datetime.datetime.now().date()

        # ---------------------------------- API CALL ----------------------------------
        # Get all active users
        # ---------------------------------- API CALL ----------------------------------
        
        responseHumanService = Blue_Ant_rest_api_request(URL.BLUEANT_REST_PROJECTRESOURCE, 'GET')
        BlueAntUserDataJson = json.loads(responseHumanService.content)
        # Create a list of all useres where leaveDate is not in the user
        activeUserID = [user['id'] for user in BlueAntUserDataJson["persons"] if "leaveDate" not in user]
        logging.info(f"The active users from the Blue Ant API {URL.BLUEANT_REST_PROJECTRESOURCE} were successfully stored in the list")
        
        # ---------------------------------- API CALL ----------------------------------
        # Get all active projects
        # ---------------------------------- API CALL ----------------------------------
        
        responseProjects = Blue_Ant_rest_api_request(URL.BLUEANT_REST_PROJECTS, 'GET')
        BlueAntProjectData = json.loads(responseProjects.content)
        # Create a list of all projects where end is not in the past
        activeProjectList = [project["id"] for project in BlueAntProjectData["projects"] if datenow < datetime.datetime.strptime(project["end"], '%Y-%m-%d').date()]
        logging.info(f"The active projects from the Blue Ant API {URL.BLUEANT_REST_PROJECTS} were successfully stored in the list")

        # ---------------------------------- API CALL ----------------------------------
        # Get all project Resources for each user and project and store the response in a XML. 
        # The request are send in parallel via asyncio.
        # The XML will be uploaded to the Azure Blob Storage.        
        # ---------------------------------- API CALL ----------------------------------
        
        sessionID = init_BA_session()
        BlueAntheaders = get_BA_SOAP_headers()
        root = ElementTree.Element('root')
        ns = {'ns9': 'http://project.blueant.axis.proventis.net/'}

        tasks = []

        for user in activeUserID:
            for project in activeProjectList:
                task = asyncio.create_task(fetch_project_resource(sessionID, user, project, BlueAntheaders, ns))
                tasks.append(task)
        
        # Collect the responses and append them to the root element
        responses = await asyncio.gather(*tasks)
        for response in responses:
            if response is not None:
                root.extend(response)
        
        new_xml_data = ElementTree.tostring(root)
        logging.info(f"The Data for the container {CONTAINER} was successfully retrieved from the Blue Ant API.")

        # -------------------- Upload the XML to the Azure Blob Storage --------------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(), new_xml_data, create_file_name('.xml'))

        close_BA_session(sessionID)

        return func.HttpResponse("Function was successfully executed", status_code=200)

    except Exception as e:
        error_traceback = traceback.format_exc()
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)