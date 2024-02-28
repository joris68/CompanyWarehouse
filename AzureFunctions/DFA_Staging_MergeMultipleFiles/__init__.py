# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
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


def main(name: str) -> str:
    connection_string = os.environ["STAGING_connectionString"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Container und Verzeichnis definieren
    container_name = "blueantprojectresource"
    directory_name = "merge/"

    # Blob Container und Verzeichnis-Client erstellen
    container_client = blob_service_client.get_container_client(container_name)

    root = ElementTree.Element('root')

    # Liste der XML-Dateien im Verzeichnis abrufen
    blob_list = container_client.list_blobs(name_starts_with=directory_name)

    for blob in blob_list:
        # XML-Dateien lesen
        blob_client = container_client.get_blob_client(blob.name)

        blob_file = blob_client.download_blob()

        blob_content = blob_file.content_as_text()

        xml_content = ElementTree.fromstring(blob_content)

        root.extend(xml_content)
        # XML-Dateien löschen
        blob_client.delete_blob()

    # XML-Dateien zusammenführen

    new_xml_data = ElementTree.tostring(root)
#    merged_blob_client = directory_client.get_blob_client(merged_blob_name)
#    merged_blob_client.upload_blob(merged_xml, overwrite=True)
    upload_blob_to_storage(f'{container_name}/' + get_relative_blob_path(), new_xml_data, create_file_name('.xml') )

    return 1
