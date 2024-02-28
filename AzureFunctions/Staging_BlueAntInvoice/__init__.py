#Author: Pascal Schütze
import logging
import requests
import azure.functions as func
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, init_BA_session, close_BA_session, get_BA_SOAP_headers
import traceback
import config_urls as URL


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantinvoice'

        sessionID = init_BA_session()
        
        #---------------------------API CALL -------------------------------------------

        headers = get_BA_SOAP_headers()

    
        # Construct the SOAP payload for GetProjectTypes request
        payload = f"""<soapenv:Envelope  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:invoice="http://invoice.blueant.axis.proventis.net/">
        <soapenv:Header/>
            <soapenv:Body>
                <invoice:getInvoiceRequestParameter>
                    <invoice:sessionID>{sessionID}</invoice:sessionID>
                    <invoice:invoice xsi:nil="true"/>
                    <invoice:proposal xsi:nil="true"/>
                    <invoice:invoiceDate xsi:nil="true"/>
                    <invoice:invoiceStateID xsi:nil="true"/>
                    <invoice:project xsi:nil="true"/>
                    <invoice:quote xsi:nil="true"/>
                    <invoice:includeDocuments xsi:nil="true"/>
                </invoice:getInvoiceRequestParameter>
            </soapenv:Body>
        </soapenv:Envelope>"""

        # Send the POST request with the constructed SOAP payload and headers
        response = requests.post(URL.BLUEANT_INVOICESERVICE, headers=headers, data=payload)

        if response.status_code != 200:
            logging.error("The response returned the wrong status code")

            raise Exception("The response returned the wrong status code")

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")

        #---------------------------API CALL ----------------------------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(),   response.content, create_file_name(".xml") )

        close_BA_session(sessionID)

        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

       # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)

