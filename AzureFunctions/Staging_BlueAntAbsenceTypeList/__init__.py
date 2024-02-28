
import logging
from xml.etree import ElementTree as ET
import traceback
import requests
import config_urls as URL
import azure.functions as func
from common import get_relative_blob_path, create_file_name, init_BA_session, close_BA_session, upload_blob_to_storage, get_BA_SOAP_headers
#hier ist das start datum fest immer vom 2023-01-01

def main(req: func.HttpRequest) -> func.HttpResponse:

    try: 

        CONTAINER = 'blueantabsencetypes'

        sessionID = init_BA_session()

        #-------------------------------API Call---------------------------------------------------
        #soap API Information
        absencetypes = ['0','2','3']

        root = ET.Element('root')

        headers = get_BA_SOAP_headers()

        for types in absencetypes:

            payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:absence=\"http://absence.blueant.axis.proventis.net/\"\r\n xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n <absence:GetAbsenceTypeListRequestParameter>\r\n<absence:sessionID>{}</absence:sessionID>\r\n<absence:all></absence:all>\r\n         <absence:type>{}</absence:type>\r\n </absence:GetAbsenceTypeListRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>".format(sessionID,types)      
        
            response = requests.request("POST", URL.BLUEANT_ABSENCESERVICE, headers=headers, data=payload)

            if response.status_code != 200:
                logging.error("The response returned the wrong status code")

                raise Exception("The response returned the wrong status code")

            xml_data = ET.fromstring(response.content)

            root.extend(xml_data)

        logging.info("Response bekommen")
        
        merged_content = ET.tostring(root)

        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")

        #-------------------------------API Call---------------------------------------------------

        upload_blob_to_storage(f'{CONTAINER}/'+get_relative_blob_path(), merged_content , create_file_name('.xml') )

        close_BA_session(sessionID)

        return func.HttpResponse("Fucntion was successfully executed", status_code=200)
    
    except Exception as e:

         # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)


