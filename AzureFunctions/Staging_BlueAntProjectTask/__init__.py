import json
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, Blue_Ant_rest_api_request, Blue_Ant_rest_api_request
import config_urls as URL
import traceback
import azure.functions as func
import os


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantprojecttask'
        
        # -------------------------- API CALL--------------------------------

        # Get all projects
        projectresponse = Blue_Ant_rest_api_request(URL.BLUEANT_REST_PROJECTS, 'GET')

        BlueAntUserProjectData = json.loads(projectresponse.content)

        ProjectIDsList = []
        ProjectTaskList = []

        # Get all project ids
        for project in BlueAntUserProjectData["projects"]:        
            ProjectIDsList.append(project["id"])

        # Get all tasks information for each project   
        for projectid in ProjectIDsList:

            url = URL.BLUEANT_REST_PLANNINGENTRIES.format(projectid)

            projecttaskresponse = Blue_Ant_rest_api_request(url, "GET")
            
            jsondata = json.loads(projecttaskresponse.text) 

            jsondata["projectbk"] = projectid # Add project id to json data becuase it is not included in the response

            ProjectTaskList.append(jsondata)
        
        jsonString = json.dumps(ProjectTaskList)
        
        # -------------------------- API CALL--------------------------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(), jsonString,  create_file_name('.json'))

        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:

         # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)


