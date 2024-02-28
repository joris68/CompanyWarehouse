#Autor: Joris/Pascal
import logging
import requests
import azure.functions as func
from common import init_BA_session, close_BA_session , get_BA_SOAP_headers, upload_blob_to_storage, get_relative_blob_path, create_file_name
import traceback
import config_urls as URL


#hier ist das start datum fest immer vom 2023-01-01

def main(req: func.HttpRequest) -> func.HttpResponse:

    try: 
        CONTAINER = 'blueantbudgetproperties'

        sessionID = init_BA_session()
        
    #-----------------------API CALL---------------------------------------------------------
        # querying BudgetService

        headers = get_BA_SOAP_headers()

        payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:budget=\"http://budget.blueant.axis.proventis.net/\"\r\n\r\n xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n\r\n<soapenv:Header/>\r\n\r\n<soapenv:Body>\r\n\r\n<budget:getBudgetPropertiesRequestParameter>\r\n\r\n<budget:sessionID>{}</budget:sessionID>\r\n\r\n<budget:projectID></budget:projectID>\r\n\r\n</budget:getBudgetPropertiesRequestParameter>\r\n\r\n</soapenv:Body>\r\n\r\n</soapenv:Envelope>".format(sessionID)


        response = requests.request("POST", URL.BLUEANT_BUDGETSERVICE, headers=headers, data=payload)

        if response.status_code != 200:
            logging.error("The response returned the wrong status code")

            raise Exception("The response returned the wrong status code")

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")
    #-----------------------API CALL---------------------------------------------------------


    # now we upload it into storage accout
        upload_blob_to_storage(f'{CONTAINER}/'+ get_relative_blob_path(),  response.content, create_file_name('.xml'))

        close_BA_session(sessionID)

        return func.HttpResponse("Fucntion was successfully executed", status_code=200)
    
    except Exception as e:

         # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
