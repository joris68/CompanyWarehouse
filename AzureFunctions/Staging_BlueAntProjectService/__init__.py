from datetime import datetime
import logging
import requests
import config_urls as URL
from common import get_relative_blob_path, create_file_name, upload_blob_to_storage, init_BA_session, close_BA_session, get_BA_SOAP_headers
import traceback

import azure.functions as func

#hier ist das start datum fest immer vom 2023-01-01

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantprojectservice'

        # generating folder-path name  and blob name
        
        
        #initializing a session ID for Blue Ant
        sessionID = init_BA_session()

        #------------------- API CALL ------------------
        
        today = datetime.today()

        # Format the date as yyyy-mm-dd
        currentDate = today.strftime('%Y-%m-%d') 

        payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:proj=\"http://project.blueant.axis.proventis.net/\" xmlns:base=\"http://base.blueant.axis.proventis.net/\"><soapenv:Header/><soapenv:Body><proj:getProjectListRequestParameter><proj:sessionID>{}</proj:sessionID><proj:projectFilter><proj:projectName></proj:projectName><proj:projectNumber></proj:projectNumber><proj:projectID></proj:projectID><proj:fromDate>2023-01-01</proj:fromDate><proj:toDate>{}</proj:toDate><proj:ownProjectsOnly></proj:ownProjectsOnly><proj:offerNumber></proj:offerNumber><proj:ticketsOnly></proj:ticketsOnly><proj:foreignKey><base:foreignID></base:foreignID><base:foreignSystem></base:foreignSystem></proj:foreignKey><proj:includeArchivedProjects></proj:includeArchivedProjects><!--Optional:--><proj:baselineExists></proj:baselineExists><!--Optional:--><proj:includeTemplates></proj:includeTemplates></proj:projectFilter></proj:getProjectListRequestParameter></soapenv:Body></soapenv:Envelope>".format(sessionID,currentDate)

        headers = get_BA_SOAP_headers()

        response = requests.request("POST", URL.BLUEANT_PROJECTSERVICE, headers=headers, data=payload)

        if response.status_code != 200:

            logging.error("The response returned the wrong status code")

            raise Exception("The response returned the wrong status code")


        logging.info(f"The Data for the container {CONTAINER} was successfully from the Blue Ant API retrieved.")

        #------------------- API CALL ------------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(),  response.content, create_file_name('.xml'))


        close_BA_session(sessionID)

        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)

    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)