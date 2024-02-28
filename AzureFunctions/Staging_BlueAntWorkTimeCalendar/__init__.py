#Author: Pascal Schütze
from xml.etree import ElementTree as ET
import logging
import requests
import azure.functions as func
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, init_BA_session, close_BA_session, get_BA_SOAP_headers
import config_urls as URL
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try: 
        
        #-------------------------------------------------------------------------------------------------------------------
        # Send request to BlueAnt to retrieve the session ID
        #-------------------------------------------------------------------------------------------------------------------
        
        
        CONTAINER = 'blueantworktimecalendar'

        sessionID = init_BA_session()
                
        #-------------------------------------------------------------------------------------------------------------------
        # Send request to MasterDataService to retrieve the project types
        #-------------------------------------------------------------------------------------------------------------------
        
        headers = get_BA_SOAP_headers()
    
        # Construct the SOAP payload for GetProjectTypes request
        payload = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:project="http://project.blueant.axis.proventis.net/"
        xmlns:base="http://base.blueant.axis.proventis.net/">
        <soapenv:Header/>
        <soapenv:Body>
            <project:GetWtCalendarsRequestParameter>
                <project:sessionID>{sessionID}</project:sessionID>
                <project:staffID></project:staffID>
                <project:fromDate></project:fromDate>
                <project:toDate></project:toDate>        
            </project:GetWtCalendarsRequestParameter>
        </soapenv:Body>
        </soapenv:Envelope>"""
        
        # Send the POST request with the constructed SOAP payload and headers
        response = requests.post(URL.BLUEANT_MASTERDATASERVICE, headers=headers, data=payload)
        
        if response.status_code != 200:
            logging.error("The response returned the wrong status code")
            raise Exception("The response returned the wrong status code")
        
        logging.info(f'The response Status Code from MasterDataService: {response.status_code}')
        
        #-------------------------------------------------------------------------------------------------------------------
        # Write response to blob storage container 'blueantworktimecalendar'
        #-------------------------------------------------------------------------------------------------------------------
        
        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(),   response.content, create_file_name(".xml") )

        close_BA_session(sessionID)

        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)

    except Exception as e:

       # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
        