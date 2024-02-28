
import logging
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, Blue_Ant_rest_api_request
import config_urls as URL
import traceback
import azure.functions as func



def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantprojectsrest'

        #-----------API CALL -------------------

        response = Blue_Ant_rest_api_request(URL.BLUEANT_REST_PROJECTS, 'GET')

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")
        #-----------API CALL -------------------

        upload_blob_to_storage(f'{CONTAINER}/'+ get_relative_blob_path(),  response.content,  create_file_name('.json'))

        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

         # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
    

   