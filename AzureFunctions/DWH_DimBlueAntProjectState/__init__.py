import logging
from datetime import datetime
import hashlib
from xml.etree import ElementTree as ET
import json

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback
import pytz


def main(req: func.HttpRequest) -> func.HttpResponse:
   
    try:
    
        # -----------------------------------------------------------
        # Load the latest json-file from the staging area
        # -----------------------------------------------------------
        timezone = pytz.timezone('Europe/Berlin')
        now = datetime.now(timezone)
        CONTAINER = 'blueantprojectstate'
    
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()  
        data = json.loads(blob_content)
        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

        statebk_list = [] # for field id in staging
        statename_list = [] # for field description in staging
        hashkeyvalue_list = [] 
        hashkeybk_list = [] 
        datetime_list =[] 
        for item in data["projectStatus"]:
            statebk = item["id"]
            statename = item["text"]

            statebk_list.append(statebk)
            statename_list.append(statename)
            #hashkeybk und haskeyvalue
            bk=str(statebk)
            hash_objectbk = hashlib.sha256(bk.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hashkeybk_list.append(hex_digbk)

            value= str(statename)
            hash_objectvalue = hashlib.sha256(value.encode())
            hex_digvalue = hash_objectvalue.hexdigest()
            hashkeyvalue_list.append(hex_digvalue)
            datetime_list.append(now)
        
        dataset = list(zip(statebk_list, statename_list, hashkeybk_list, hashkeyvalue_list,datetime_list,datetime_list))

        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntProjectState"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntProjectState"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        
        insert_tmp_statement = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] ([ProjectStateBK],[ProjectStateName],  [HashkeyBK] , [HashkeyValue] , [InsertDate], [UpdateDate]) VALUES (?,?, ?,?,?,?)"
        
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        cursor.execute(f"""
        DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

        MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS tgt
        USING [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS src
        ON (tgt.HashkeyBK = src.HashkeyBK)
        WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue 
        THEN UPDATE SET tgt.HashKeyValue = src.HashKeyValue,
                        tgt.ProjectStateBK = src.ProjectStateBK,
                        tgt.ProjectStateName = src.ProjectStateName,
                        tgt.UpdateDate = src.UpdateDate
        WHEN NOT MATCHED THEN
            INSERT (ProjectStateBK, ProjectStateName, HashkeyBK, HashkeyValue, InsertDate, UpdateDate)
            VALUES (src.ProjectStateBK, src.ProjectStateName, src.HashkeyBK, src.HashkeyValue, src.InsertDate, src.UpdateDate)
        OUTPUT $action AS [Status] INTO @MergeLog;

        INSERT INTO [tmp].[changed] ([Status], Anzahl)
        SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
        GROUP BY [Status];
        """)
        
        conn.commit()
        logging.info("The Merge statement was executed successfully.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=True)

        close_DWH_DB_connection(conn, cursor)  
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)