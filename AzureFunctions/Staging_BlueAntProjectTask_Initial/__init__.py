import azure.functions as func
import pandas as pd
from common import get_latest_blob_from_staging, get_time_in_string, Blue_Ant_rest_api_request,upload_blob_to_storage
import config_urls as URL
import traceback
import json

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        now = get_time_in_string()[0].date()

        CONTAINER = 'blueantprojecthistory'

        blob_content = get_latest_blob_from_staging(CONTAINER)

        xl = pd.read_excel(blob_content.content_as_bytes(),sheet_name="sheet1",header=0,na_filter = False)

        # Name der Spalte, die du auslesen möchtest
        column_name = 'ProjektBK'  

        # Extrahiere die Werte aus der Spalte in eine Liste
        project_list = xl[column_name].tolist()

        ProjectTaskList = []

        # Get all tasks information for each project   
        for projectid in project_list:

            url = URL.BLUEANT_REST_PLANNINGENTRIES.format(projectid)

            projecttaskresponse = Blue_Ant_rest_api_request(url, "GET")
            
            jsondata = json.loads(projecttaskresponse.text) 

            jsondata["projectbk"] = projectid # Add project id to json data becuase it is not included in the response

            ProjectTaskList.append(jsondata)
        
        jsonString = json.dumps(ProjectTaskList)
        
        # -------------------------- API CALL--------------------------------

        CONTAINER2 = 'initialloads'

        upload_blob_to_storage(f'{CONTAINER2}',  jsonString, str(now)+('-blueantprojecttask.json'))

        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:

         # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)