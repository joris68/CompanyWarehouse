# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as df
import datetime
from common import Blue_Ant_rest_api_request,upload_blob_to_storage,get_relative_blob_path, create_file_name
import traceback
import os
from azure.storage.blob import BlobServiceClient
from xml.etree import ElementTree
import config_urls as URL

def orchestrator_function(context: df.DurableOrchestrationContext):
    
    try:
        #    result1 = yield context.call_activity('Hello', "Tokyo")
        #    result2 = yield context.call_activity('Hello', "Seattle")
        #    result3 = yield context.call_activity('Hello', "London")
        #    return [result1, result2, result3]

        # ----------------- API CALL --------------------

        userURL = URL.BLUEANT_REST_PROJECTRESOURCE
        userreponse = Blue_Ant_rest_api_request(userURL, 'GET')

        BlueAntUserJsonData = json.loads(userreponse.content)

        activeUserID = []

        for user in BlueAntUserJsonData["persons"]:
            if "leaveDate" not in user:
                activeUserID.append(user["id"])

        logging.info("activeUser in Liste gespeichert")
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        logging.error("Fehler beim hinzufügen der aktiven User")

        logging.error("Fehler:"+error_traceback)

        raise Exception("Fehler:"+error_traceback)
    

    for user in activeUserID:
        result= yield context.call_activity('DFA_Staging_ProjectResources',user)
    
    result2 = yield context.call_activity('DFA_Staging_MergeMultipleFiles')


main = df.Orchestrator.create(orchestrator_function)