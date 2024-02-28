
import logging
from xml.etree import ElementTree as ET
import requests
import traceback
import azure.functions as func
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, init_BA_session, close_BA_session, get_BA_SOAP_headers
import config_urls as URL

#hier ist das start datum fest immer vom 2023-01-01

def main(req: func.HttpRequest) -> func.HttpResponse:


    try:

        CONTAINER = 'blueantparticipationprojects'

        sessionID = init_BA_session()

    #---------------------------------API CALL -----------------------------------
        # querying BudgetService

        headers = get_BA_SOAP_headers()

        business_payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:project=\"http://project.blueant.axis.proventis.net/\"\r\n xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n <project:GetParticipationProjectsRequestParameter>\r\n<project:sessionID>{}</project:sessionID>   \r\n</project:GetParticipationProjectsRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>".format(sessionID)


        business_response = requests.request("POST", URL.BLUEANT_PROJECTSERVICE, headers=headers, data=business_payload)

        if business_response.status_code != 200:
            logging.error("The response returned the wrong status code")

            raise Exception("The response returned the wrong status code")

        root = ET.fromstring(business_response.content)
    
        project_ids = [elem.text for elem in root.findall('.//{http://cost.blueant.axis.proventis.net/}projectID')]
        project_ids = set(list(project_ids))

        logging.info("Business Logic was successfully executed")

    #---------------------------------API CALL -----------------------------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(), business_response.content, create_file_name('.xml') )

        close_BA_session(sessionID)
    
        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
