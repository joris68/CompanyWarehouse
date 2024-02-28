import pytz
from datetime import datetime
from typing import List
import logging
from azure.storage.blob import BlobServiceClient
import os
from xml.etree import ElementTree as ET
import requests
import pyodbc
import json



def Blue_Ant_rest_api_request(blueant_rest_api_url, request_type):
    # This function is used to make a request to the BlueAnt REST API
    # it raises an exception if the request returns a status code other than 200
      
    # variables for the request
    BlueAntRestpayload = {}
    Blue_ant_rest_auth = os.environ['BlueAntRestAPIToken'] #defined in local.settings.json or in the Azure Function App settings

    # headers for the request 
    BlueAntRestheaders = {
        'Authorization': 'Bearer ' + Blue_ant_rest_auth
    }
    
    RESTBlueAntProjectResponse = requests.request(request_type, blueant_rest_api_url, headers=BlueAntRestheaders, data=BlueAntRestpayload)

    if RESTBlueAntProjectResponse.status_code == 200:
        logging.info("The API request was successfull, returning the response.")
        return RESTBlueAntProjectResponse
    else:
        logging.error("The request returned the wrong Status Code, raising an Exception.")
        raise Exception("The request returned the wrong Status Code aborting the function.")



def get_BA_SOAP_headers():
    # This function returns the headers for a SOAP request to the BlueAnt SOAP API
    headers = {
        'Content-Type': 'text/xml',
        'SOAPAction': 'POST'
    }
    logging.info("The headers for the SOAP request were successfully retrieved.")
    return headers



def jira_api_request(jira_rest_api_url, request_type):
    # This function is used to make a request to the Jira API
    # it raises an exception if the request returns a status code other than 200
    
    jiraAuth = os.environ['JiraAuth'] #defined in local.settings.json or in the Azure Function App settings

    # headers for the request
    headers_get = {
        "Authorization": f'Basic {jiraAuth}'
    }

    response = requests.request(request_type ,url=jira_rest_api_url, headers= headers_get)

    if response.status_code == 200:
        # IF Case only for staging worklogs, othwerwise would be to many log entries
        if 'api/3/search?maxResults=100&startAt=' or 'api/3/issue/' in jira_rest_api_url: 
            return response
        else:
            logging.info("The API request was successful, the function returns the response.")
            return response
    else:
        logging.error("The request returned the wrong Status Code, raising an Exception.")
        raise Exception("The request returned the wrong Status Code, aborting the function.")



def insert_into_loadlogging(table_schema, table_name, cursor_object, conn_object):
    # This function inserts a new row into the LoadLogging table
    # it contains the schema and table name of the table that was loaded 
    # and the current time as the start time of the load process, 
    # inserted rows, updated rows and deleted rows

    try:
        cursor_object.execute(f"""
        INSERT INTO [logging].[LoadLogging] ([SchemaName], [TableName], [StartDateTime], [EndDateTime], [InsertedRows], [UpdatedRows], [DeletedRows])
        VALUES(N'{table_schema}',N'{table_name}',GETDATE(),NULL,NULL,NULL,NULL)
        """)

        logging.info("The LoadLogging table is updated with the current time and added, updated or deleted rows.")
        conn_object.commit()
    except:
        logging.error("Error occured during the Insert into the LoadLogging table, raising an Exception.")
        raise Exception("Error occured during the Insert into the LoadLogging table, aborting the function")



def init_DWH_Db_connection():
    # This function returns a connection and cursor object for the DWH
    
    try:
        DWHconnection_string = os.environ['DBDWHConnectionString']
        conn = pyodbc.connect(DWHconnection_string)
        
        logging.info("The DWH connection was established and cursor connection object was successfully created.")
        return  (conn ,conn.cursor())
    except:
        logging.error("Failure to establish a connection to the DWH, raising an Exception")
        raise Exception("Failure to establish a connection to the DWH, aborting the Function")



def close_DWH_DB_connection(conn , cursor):
    # This function disconnects the DWH connection and cursor object for the DWH

    try:
        cursor.close()
        conn.close()
        logging.info("The DWH connection was successfully disconnected.")
    except:
        logging.error("Error to disconnect the DWH connection, raising an Exception")
        raise Exception("Error to disconnect the DWH connection, aborting the Function")
    
    
    
def truncate_tmp_table(table_schema, table_name, cursor, conn):
    # This function truncates the tmp table
    
    try: 
        cursor.execute(f"""TRUNCATE TABLE [{table_schema}].[{table_name}]""")
        conn.commit()
        logging.info(f"The table {table_schema}.{table_name} was successfully truncated.")
    except:
        logging.error(f"Error occured while truncating the tmp table {table_schema}.{table_name}, raising an Exception")
        raise Exception(f"Error occured while truncating the tmp table {table_schema}.{table_name}, aborting the Function") 



def insert_into_tmp_table(dataset, insert_tmp_statement, cursor,  conn):
    # This function inserts the dataset into the tmp table by fast_executemany
    
    try:
        cursor.fast_executemany = True
        cursor.executemany(insert_tmp_statement, dataset)
        conn.commit()
        logging.info("Bulk insert into temporary table succeeded: %d rows inserted.", len(dataset))
    except Exception as e:
        error_msg = "Failed to insert data into temporary table. Error: %s" % str(e)
        logging.error(error_msg)
        raise Exception(error_msg)
        
def lookup_tmp_table(query, cursor, conn):
    # This function returns a lookup table from the DWH and updates the tmp table 
    
    try:
        cursor.execute(query)
        conn.commit()
        logging.info("The lookup was successful, updated the tmp table.")
    except Exception as e: 
        error_msg = "Failed to update the tmp table with the lookup table. Error: %s" % str(e)
        logging.error(error_msg)
        raise Exception(error_msg)


def get_time_in_string():
    # This function returns the current time in variables 
    
    timezone = pytz.timezone('Europe/Berlin')
    now = datetime.now(timezone)
    
    # Add leading zeros to the time variables, except for the year
    minute = f'{now.minute:02d}'
    hour = f'{now.hour:02d}'
    day = f'{now.day:02d}'
    month = f'{now.month:02d}'
    year = str(now.year)

    
    return [now, timezone, minute, hour, day, month, year]
    


def get_relative_blob_path():
    # This function returns the relative blob path for the current time
    
    now, timezone, minute, hour, day, month, year = get_time_in_string()
    logging.info("Got the time string for the relative Blob path")
    return year + '/' + month + '/' + day 



def create_file_name(file_Extension):
    # This function returns the file name for the current time
    
    now, timezone, minute, hour, day, month, year = get_time_in_string()
    logging.info("Got the time string for the file extension")
    return hour + minute + file_Extension

    

def upload_blob_to_storage(container_path, blob_data, blob_name):
    """This Function uploads a blob to the Staging Blob Storage container.
    
    Args:
        container_path (str): The path to the container where the blob will be uploaded.
        blob_data (bytes): The blob data to be uploaded.
        blob_name (str): The name of the blob to be uploaded."""

    try:
        app_setting = 'STAGING_connectionString'
        connection_string = os.environ[app_setting] 

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_path) 
        output_blob = container_client.get_blob_client(blob_name)

        # Upload the blob data to the container, overwriting if the blob already exists.
        output_blob.upload_blob(blob_data,overwrite=True)

        logging.info(f"Blob was successfully uploaded to Staging Blob Storage {container_path}")
    except Exception as e:
        logging.error("An error occurred during blob upload to Staging Blob Storage: %s", str(e))
        raise Exception("An error occurred during blob upload to Staging Blob Storage")


    
def get_latest_blob_from_staging(container_name, folder_name = None, datalake_connection = None):
    """ This function retrieves the latest blob from the specified Staging Blob Storage container.
    
    Args:
        container_name (str): The name of the container.
        folder_name (str, optional): The name of the folder within the container (if any).
    Returns:
        BlobClient: The BlobClient object representing the latest blob."""

    try:
        # Retrieve the connection string from the environment variables, for now we have two connections: Dataverse and Staging (Default)
        if datalake_connection == None:
            connection_string = os.environ['STAGING_connectionString']
        elif datalake_connection == "dataverse":
            connection_string = os.environ['DataverseConnectionString']
            
        # Build a connection to the Blob Storage    
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # Get the container client based on the container name
        container_client = blob_service_client.get_container_client(container_name)
        
        # List all blobs in the container and find the latest one based on last_modified time.
        # Note: Folder are in Datalakes not real folders, they are prefixes in the blob name
        if folder_name is not None:
            blob_list = container_client.list_blobs(name_starts_with=folder_name)
        else: 
            blob_list = container_client.list_blobs()

        last_modified_blob = None
        last_modified_time = None
        
        for blob in blob_list:
            if last_modified_time is None or blob.last_modified > last_modified_time:
                last_modified_blob = blob
                last_modified_time = blob.last_modified

        # Retrieve the BlobClient for the latest blob.
        blob_client = container_client.get_blob_client(last_modified_blob)

        logging.info(f"The latest blob with the name : {last_modified_blob.name} was successfully retrieved from Staging Blob Storage")
        return blob_client.download_blob()
    
    except Exception as e:
        logging.error("An error occurred during blob retrieval from Staging Blob Storage: %s", str(e))
        raise Exception("An error occurred during blob retrieval from Staging Blob Storage")



def init_BA_session() -> str:
    """This function Initiates a BlueAnt session by sending a login request.
    
    Returns:
        str: The session ID retrieved from the response."""
    
    try:
        BlueAntPassword = os.environ['BlueAntPassword']
        BlueAntLogin = os.environ['BlueAntLogin']
        
        url = "https://ceteris.blueant.cloud/services/BaseService/"
        
        payload = (
            f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" "
            f"xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n <soapenv:Header/>\r\n "
            f"<soapenv:Body>\r\n<base:LoginRequestParameter>\r\n<base:username>{BlueAntLogin}</base:username>"
            f"\r\n<base:password>{BlueAntPassword}</base:password>\r\n</base:LoginRequestParameter>\r\n</soapenv:Body>\r\n"
            f"</soapenv:Envelope>"
        )

        header = {
            'Content-Type': 'text/xml',
            'SOAPAction': 'POST'
        }
        
        response = requests.request("POST", url, headers=header, data=payload)
        tree = ET.fromstring(response.content)

        if response.status_code == 200:
            session_id = tree.find('.//{http://base.blueant.axis.proventis.net/}sessionID').text
            logging.info("BlueAnt session was successfully initiated. Session ID: %s", session_id)
            return session_id
        else:
            error_msg = "The response for the Session ID was not correct."
            logging.error(error_msg)
            raise Exception(error_msg)
    
    except Exception as e:
        logging.error("Error while initiating BlueAnt session: %s", str(e))
        raise
   


def close_BA_session(sessionID):
    """This Function closes a BlueAnt session using the provided session ID.
    
    Args:
        sessionID (str): The session ID to close."""

    try:
        header = {
            'Content-Type': 'text/xml',
            'SOAPAction': 'POST'
        }
        
        url = "https://ceteris.blueant.cloud/services/BaseService/"
        
        payload = (
            f"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" "
            f"xmlns:base=\"http://base.blueant.axis.proventis.net/\">\r\n<soapenv:Header/>\r\n<soapenv:Body>\r\n"
            f"<base:LogoutRequestParameter>\r\n<base:sessionID>{sessionID}</base:sessionID>\r\n"
            f"</base:LogoutRequestParameter>\r\n</soapenv:Body>\r\n</soapenv:Envelope>"
        )
        
        response = requests.request("POST", url, headers=header, data=payload)
        
        if response.status_code == 200:
            logging.info("BlueAnt session was successfully closed. Session ID: %s", sessionID)
        else:
            logging.error("Failed to close BlueAnt session. Session ID: %s", sessionID)
            
    except Exception as e:
        logging.error("Error while closing BlueAnt session: %s", str(e))



def DWH_table_logging(table_name, cursor_object, connection_object, is_Dimension= True):
    """This function logs data manipulation and status into a logging table for a specified table.
    
    Args:
        table_name (str): The name of the table for which logging is performed.
        cursor_object: A database cursor object for executing SQL queries.
        connection_object: A database connection object for committing changes.
        is_Dimension (bool, optional): A boolean indicating whether the table is a dimension (default is True).
        
    This function performs the following steps:
    1. Fetches data from a temporary table containing change information.
    2. Updates the logging information for the specified table with the number of inserted and updated rows.
    3. Truncates the temporary change tracking table.
    4. If the table is marked as a dimension (is_Dimension=True), executes a stored procedure to handle unknown dimension members.
    5. Logs the success of the logging process."""

    try:
        # Fetch data from a temporary table containing change information
        sql_query = "SELECT Status, Anzahl FROM [tmp].[changed];"
        data = []
        for row in cursor_object.execute(sql_query):
            data.append(row)

        # Update the logging information for the table
        sql_loggingquery = f"""
        DECLARE @MaxRowID int = (SELECT MAX(RowID) FROM [logging].[LoadLogging] WHERE TableName = N'{table_name}');

        UPDATE [logging].[LoadLogging]
        SET [EndDateTime] = GETDATE(),
            [InsertedRows] = ?,
            [UpdatedRows] = ?,
            [DeletedRows] = 0
        WHERE [TableName] = N'{table_name}'
        AND RowID = @MaxRowID;

        TRUNCATE TABLE [tmp].[changed];"""
        
        InsertAnzahl = 0
        UpdateAnzahl = 0
        for row in data:
            Status = row[0]
            if Status == 'INSERT':
                InsertAnzahl = row[1]
            if Status == 'UPDATE':
                UpdateAnzahl = row[1]

        cursor_object.execute(sql_loggingquery, (InsertAnzahl, UpdateAnzahl))
        connection_object.commit()

        if is_Dimension:
            # Execute a stored procedure to handle unknown dimension members
            cursor_object.execute(f"""
            EXEC [dbo].[prc_AddUnknownDimensionMembersDynamic]
                @SchemaName = 'dwh',
                @TableName = '{table_name}';
            """)
            connection_object.commit()
            logging.info("Executed unknown member handling for the Dimension Logging")

        logging.info("The Logging Process was completed successfully.")

    except Exception as e:
        # Handle exceptions and provide appropriate error message
        logging.error("An error occurred during the logging process into the Logging table")
        raise Exception("An error occurred during the logging process into the Logging table")
    

#MC 28.01.2024: wenn in jeder function die neue function verwendet wird, fällt das raus. Kann ich später nochmal gucken.    
#DBR 30.01.2024: ?????????    
#Bekomme die SprintIDs vom aktiven Sprint und von den letzten 2 Wochen als Liste.
def get_last_two_SprintID():
        #ich brauche die active sprint id von der api.
    #Jira credenztials from Key vault
    jiraAuth = os.environ['JiraAuth']

    #SprintID herausfinden für aktuellen Sprint damit ich den aktuellen Sprint und die letzten beiden lade.
    sprinturl = "https://ceteris.atlassian.net/rest/agile/1.0/board/6/sprint"

    sprintpayload={}
    sprintheaders = {
        'Authorization': 'Basic {}'.format(jiraAuth)
    }

    sprintresponse = requests.request("GET", sprinturl, headers=sprintheaders, data=sprintpayload)

    sprintjsondata = json.loads(sprintresponse.text)

    sprints_ids = []

    for i, sprint in enumerate(sprintjsondata["values"]):
        if sprint["state"] == "active":
            active_sprint = sprint["id"]           
            if i >= 2:
                previous_sprints = sprintjsondata["values"][i-2:i]
                sprints_ids = [sprint["id"] for sprint in previous_sprints]
            sprints_ids.append(active_sprint)
            break

    return sprints_ids

def get_blob_start_with(containerName,FileNameStart):
    try:
        connection_string = os.environ['STAGING_connectionString']
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(containerName)
        
        blob_prefix = FileNameStart

        # List all blobs in the container and find the latest one based on last_modified time.
        blob_list = container_client.list_blobs()
        last_modified_blob = None
        last_modified_time = None
        
        for blob in blob_list:
            if blob_prefix in blob.name:
                if last_modified_time is None or blob.last_modified > last_modified_time:
                    last_modified_blob = blob
                    last_modified_time = blob.last_modified

        # Retrieve the BlobClient for the latest blob.
        blob_client = container_client.get_blob_client(last_modified_blob)

        logging.info(f"The latest blob with the name : {last_modified_blob.name} was successfully retrieved from Staging Blob Storage")
        return blob_client.download_blob()
    
    except Exception as e:
        logging.error("An error occurred during blob retrieval from Staging Blob Storage: %s", str(e))
        raise Exception("An error occurred during blob retrieval from Staging Blob Storage")