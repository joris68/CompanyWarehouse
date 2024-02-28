
import logging
import requests
from common import get_relative_blob_path, create_file_name, get_BA_SOAP_headers, upload_blob_to_storage, init_BA_session, close_BA_session
import config_urls as URL
import traceback
import azure.functions as func

#hier ist das start datum fest immer vom 2023-01-01

def main(req: func.HttpRequest) -> func.HttpResponse:


    try: 

        CONTAINER = 'blueantprojecttypes'

        sessionID = init_BA_session()


#------------------------API Call---------------------------------------------

        headers = get_BA_SOAP_headers()

        payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:masterdata=\"http://masterdata.blueant.axis.proventis.net/\"\r\n xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n<masterdata:GetProjectTypesRequestParameter>\r\n<masterdata:sessionID>{}</masterdata:sessionID>\r\n </masterdata:GetProjectTypesRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>".format(sessionID)

        response = requests.request("POST", URL.BLUEANT_MASTERDATASERVICE, headers=headers, data=payload)

        if response.status_code != 200:

            logging.error("The response returned the wrong status code")

            raise Exception("The response returned the wrong status code")

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant Soap API retrieved.")

        #----------------------------------------------- API Call ---------------------------------------------------

        # Upload the BA API response to storage
        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(),  response.content, create_file_name('.json'))

        close_BA_session(sessionID)

        # Return a successful HTTP response with a line break
        return func.HttpResponse("The Function was successfully executed.\n", status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)