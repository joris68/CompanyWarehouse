#Autor:MC
#from datetime import datetime as dt
#import datetime
import logging
import azure.functions as func
import requests
from xml.etree import ElementTree as ET
from common import init_BA_session, close_BA_session, get_BA_SOAP_headers, upload_blob_to_storage,get_time_in_string
import config_urls as URL
import traceback

#Ablauf der Function App: als erstes greife ich auf die 1.API zu um die SessionID zu bekommen. Denn für die weiteren API Aufrufe brauche ich die SessionID.
#dann greife ich die 2.API zu in einer verschachtelten for loop wegen billable und state. Weil es 2 Filter sind auf die ich zugreifen muss um alle Daten zu bekommen 
#die einzelnen dateien(6) werden im Container erzeugt. Dann werden die 6 Dateien gemerged zu einer einzelnen und im Unterordner erzeugt.
#dann werden die dateien(6) im Container gelöscht. Damit nur die eine Hauptdatei in den Unterordnern exisitert

#load BlueAnt Worktime from 2017 - 2022

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        now = get_time_in_string()[0].date()
        
        CONTAINER = 'initialloads'
        
        # get session ID
        sessionID = init_BA_session()

        # ----- APi CALL -----------

        #hier erstelle ich listen weil es mehrere Möglichkeiten für den API Aufruf gibt 
        #erstellt, abgerechnet, geschlossen 
        state = [0,1,2]

        #Fakturierbar/nicht fakturierbar
        billable = [0,1]

        root = ET.Element('root')

        headers = get_BA_SOAP_headers()

        for items in state:

            for bill in billable:
                
                payload = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:cost=\"http://cost.blueant.axis.proventis.net/\" xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n\r\n<soapenv:Header/>\r\n\r\n<soapenv:Body>\r\n\r\n<cost:getAllWorktimeRequestParameter>\r\n\r\n<cost:sessionID>{}</cost:sessionID> \r\n\r\n<cost:workTimeID></cost:workTimeID>\r\n\r\n<cost:fromDate>2017-01-01</cost:fromDate>\r\n\r\n<cost:toDate>2022-12-31</cost:toDate>\r\n\r\n<cost:ticketID></cost:ticketID>\r\n\r\n<cost:projectID></cost:projectID>\r\n\r\n<cost:taskID></cost:taskID>\r\n\r\n<cost:state>{}</cost:state>\r\n\r\n<cost:billable>{}</cost:billable>\r\n\r\n<cost:reasonNotAccountableID></cost:reasonNotAccountableID>\r\n\r\n<cost:exported></cost:exported>\r\n\r\n<cost:exportStartDate></cost:exportStartDate>\r\n\r\n<cost:exportEndDate></cost:exportEndDate>\r\n\r\n<cost:personNumber></cost:personNumber>\r\n\r\n<cost:personID></cost:personID>\r\n\r\n<cost:fistname></cost:fistname>\r\n\r\n<cost:lastname></cost:lastname>\r\n\r\n<cost:lastChangeStartDate></cost:lastChangeStartDate>\r\n\r\n<cost:lastChangeEndDate></cost:lastChangeEndDate>\r\n\r\n</cost:getAllWorktimeRequestParameter>\r\n\r\n</soapenv:Body>\r\n\r\n</soapenv:Envelope>".format(sessionID,items,bill)
    
                worktimeresponse = requests.request("POST", URL.BLUEANT_WORKTIME_ACCOUNTING_SERVICE, headers=headers, data=payload)

                xml_data = ET.fromstring(worktimeresponse.content)

                root.extend(xml_data)
        
        merged_content = ET.tostring(root)
        
        upload_blob_to_storage(f'{CONTAINER}', merged_content, str(now)+'blueantworklog.xml')

        logging.info("Data was successfully written to the Storage")

        close_BA_session(sessionID)

        return func.HttpResponse("Successfull ETL Process", status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)

    