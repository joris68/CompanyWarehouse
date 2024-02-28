
import logging
import requests
import azure.functions as func
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, init_BA_session, close_BA_session, get_BA_SOAP_headers
import traceback
import config_urls as URL


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantinvoicestate'

        sessionID = init_BA_session()

    #--------------API CALL  -------------------------------

        headers = get_BA_SOAP_headers()  


        payload = "<soapenv:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:masterdata=\"http://masterdata.blueant.axis.proventis.net/\"><soapenv:Header/><soapenv:Body><masterdata:GetInvoiceStatesRequestParameter><masterdata:sessionID>{}</masterdata:sessionID></masterdata:GetInvoiceStatesRequestParameter></soapenv:Body></soapenv:Envelope>".format(sessionID)

        response = requests.request("POST", URL.BLUEANT_MASTERDATASERVICE, headers=headers, data=payload)

        if response.status_code != 200:
            logging.error("The response returned the wrong status code")

            raise Exception("The response returned the wrong status code")

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")

    #--------------API CALL  -------------------------------

        upload_blob_to_storage(f'{CONTAINER}/'+ get_relative_blob_path(),  response.content, create_file_name('.xml'))

        close_BA_session(sessionID)

        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)

    

