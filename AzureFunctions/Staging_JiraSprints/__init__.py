
import logging
import traceback
import azure.functions as func
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, jira_api_request
import config_urls as URL 


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'jirasprints'
    
        #-----------API Call ------------

        response = jira_api_request(URL.JIRA_SPRINT, 'GET')

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Jira API retrieved.")

       #-----------API Call ------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(),  response.content, create_file_name('.json') )
    
        return func.HttpResponse( "The Function was successfully executed.\n"  ,status_code=200)

    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
