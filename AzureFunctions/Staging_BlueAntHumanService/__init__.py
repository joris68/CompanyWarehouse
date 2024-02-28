import logging
import azure.functions as func
import requests
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, init_BA_session, close_BA_session, Blue_Ant_rest_api_request, get_BA_SOAP_headers
import traceback
import json
import config_urls as URL

from xml.etree import ElementTree as ET



def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
    
        CONTAINER = 'blueanthumanservice'
        
        sessionID = init_BA_session()

        # ----------API CALL -----------------------

        root = ET.Element('root')

        #erstmal wird ein API Call an die Rest API gemacht weil wir da mehr Mitarbeiter bekommen.
        response = Blue_Ant_rest_api_request(URL.BLUEANT_REST_PROJECTRESOURCE, 'GET')
        
        jsondata = json.loads(response.content)

        user_list = []

        for item in jsondata["persons"]:
            if item["id"] not in user_list: 
                user_list.append(item["id"])

        for value in user_list:
            
            payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:hum=\"http://human.blueant.axis.proventis.net/\" xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n<hum:GetPersonRequestParameter>\r\n<hum:sessionID>{}</hum:sessionID>\r\n<hum:personID>{}</hum:personID>\r\n</hum:GetPersonRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>".format(sessionID,value)
            
            headers = get_BA_SOAP_headers()

            response = requests.request("POST", URL.BLUEANT_HUMANSERVICE, headers=headers, data=payload)

            if response.status_code != 200:
                logging.error("The response returned the wrong status code")

                raise Exception("The response returned the wrong status code")

            xml_data = ET.fromstring(response.content)

            root.extend(xml_data)

        merged_content = ET.tostring(root)

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")

        # ----------API CALL -----------------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(), merged_content, create_file_name('.xml') )

        close_BA_session(sessionID)
    
        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)




