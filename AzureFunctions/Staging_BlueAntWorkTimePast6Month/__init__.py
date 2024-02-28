#Autor:MC
from datetime import datetime,timedelta
import azure.functions as func
import requests
import traceback
from xml.etree import ElementTree as ET
import pytz
from dateutil.relativedelta import relativedelta
from common import get_relative_blob_path, create_file_name, init_BA_session, close_BA_session, get_BA_SOAP_headers, upload_blob_to_storage
#Ablauf der Function App: als erstes greife ich auf die 1.API zu um die SessionID zu bekommen. Denn für die weiteren API Aufrufe brauche ich die SessionID.
#dann greife ich die 2.API zu in einer verschachtelten for loop wegen billable und state. Weil es 2 Filter sind auf die ich zugreifen muss um alle Daten zu bekommen 
#die einzelnen dateien(6) werden im Container erzeugt. Dann werden die 6 Dateien gemerged zu einer einzelnen und im Unterordner erzeugt.
#dann werden die dateien(6) im Container gelöscht. Damit nur die eine Hauptdatei in den Unterordnern exisitert

#hier ist das StartDatum immer der 1. vom vorletzten Monat

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER ='blueantworktime'
    
        now_worktime = datetime.now(pytz.timezone('Europe/Berlin')).date() # aktuelles Datum
        # Erstelle ein Datum vor 6 Monaten
        six_months_ago = now_worktime - relativedelta(months=6)

        # Setze den Tag auf 1, um den ersten Tag des Monats zu erhalten
        first_day_six_months_ago = six_months_ago.replace(day=1)

        sessionID = init_BA_session()

        #hier erstelle ich listen weil es mehrere Möglichkeiten für den API Aufruf gibt 
        #erstellt, abgerechnet, geschlossen 
        state = [0,1,2]

        #Fakturierbar/nicht fakturierbar
        billable = [0,1]

        root = ET.Element('root')

        headers = get_BA_SOAP_headers()

        blueantworktimeurl = "https://ceteris.blueant.cloud/services/WorktimeAccountingService/"

        for items in state:

            for bill in billable:
                payload = (f"""
                    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                    xmlns:cost="http://cost.blueant.axis.proventis.net/" 
                        xmlns:base="http://base.blueant.axis.proventis.net/">
                            <soapenv:Header/>
                            <soapenv:Body>
                                <cost:getAllWorktimeRequestParameter>
                                <cost:sessionID>{sessionID}</cost:sessionID>
                                <cost:workTimeID></cost:workTimeID>
                                <cost:fromDate>{first_day_six_months_ago}</cost:fromDate>
                                <cost:toDate>{now_worktime}</cost:toDate>
                                <cost:ticketID></cost:ticketID>
                                <cost:projectID></cost:projectID>
                                <cost:taskID></cost:taskID>
                                <cost:state>{items}</cost:state>
                                <cost:billable>{bill}</cost:billable>
                                <cost:reasonNotAccountableID></cost:reasonNotAccountableID>
                                <cost:exported></cost:exported>
                                <cost:exportStartDate></cost:exportStartDate>
                                <cost:exportEndDate></cost:exportEndDate>
                                <cost:personNumber></cost:personNumber>
                                <cost:personID></cost:personID>
                                <cost:fistname></cost:fistname>
                                <cost:lastname></cost:lastname>
                                <cost:lastChangeStartDate></cost:lastChangeStartDate>
                                <cost:lastChangeEndDate></cost:lastChangeEndDate>
                                </cost:getAllWorktimeRequestParameter>
                            </soapenv:Body>
                            </soapenv:Envelope>""")

                worktimeresponse = requests.request("POST", blueantworktimeurl, headers=headers, data=payload)
                
                xml_data = ET.fromstring(worktimeresponse.content)

                root.extend(xml_data)

        merged_content = ET.tostring(root)

        #-----------API CAll-------------------

        upload_blob_to_storage(f'{CONTAINER}/' + get_relative_blob_path(),  merged_content, create_file_name('.xml') )
    
        close_BA_session(sessionID)

        return func.HttpResponse( "Function was successfully executed"  ,status_code=200)    

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)


    