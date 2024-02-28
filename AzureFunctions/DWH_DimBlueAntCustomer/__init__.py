#Changes: PSC
from datetime import datetime
import logging
import hashlib
import json
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string

from azure.storage.blob import BlobServiceClient
import azure.functions as func
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

   try:
    
        # -----------------------------------------------------------
        # Load the latest json-file from the staging area
        # -----------------------------------------------------------
    
        CONTAINER = 'blueantcustomer'

        now = get_time_in_string()[0]
        
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()

        data = json.loads(blob_content)

        #----------------------------------------------------------------------------------------
        # Now, retrive the data from json and pack it into variables that are loaded into DB 
        #----------------------------------------------------------------------------------------

        customerNames_list = [] # for field text in staging
        customerIds_list = [] # for field id in staging
        hashkeyvalue_list = [] # for field HashKeyValue in tmp
        hashkeybk_list = [] # for field HashKeyBK in tmp
        datetime_list = []
        x = 0 # for counting the loop

        for c in data['customers']:

            # we only want unique values
            if c['id'] not in customerIds_list:
                customerIds=c['id']
                customerNames=c['text'][:255]

                customerNames_list.append(customerNames)
                customerIds_list.append(customerIds)
                
                bk = str(customerIds)
                bk_hash = hashlib.sha256(bk.encode()).hexdigest()
                hashkeybk_list.append(bk_hash)
                
                value = str(customerNames)
                value_hash = hashlib.sha256(value.encode()).hexdigest()
                hashkeyvalue_list.append(value_hash)

                datetime_list.append(now)
                
                x=+1
            
        dataset = list(zip(customerIds_list, customerNames_list, hashkeybk_list, hashkeyvalue_list,datetime_list,datetime_list))
        
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntCustomer"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntCustomer"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        insert_tmp_statement = f""" 
            INSERT INTO {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} 
            (CustomerBK, CustomerName, HashkeyBK, HashKeyValue, InsertDate, UpdateDate)
            VALUES (?, ?, ?, ?, ?,?)
        """
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)      
        
        #----------------------------------------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table.  
        #----------------------------------------------------------------------------------------

        cursor.execute(f"""
        DECLARE @MergeLog TABLE ([Status] VARCHAR(20));
        WITH CTE AS (
            SELECT 
                CustomerBK,
                CustomerName,
                HashkeyBK,
                HashkeyValue,
                InsertDate,
                UpdateDate
            FROM {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME}

            UNION ALL

            SELECT
                25287,
                N'Ceteris AG',
                '25287',
                'Ceteris AG',
                CONVERT(DATETIME, '1900-01-01'),
                CONVERT(DATETIME, '1900-01-01'))
                
        MERGE {SCHEMA_NAME}.{TABLE_NAME} AS tgt
        USING CTE AS src
        ON (tgt.HashkeyBK = src.HashkeyBK)
        WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue THEN
            UPDATE SET tgt.CustomerName = src.CustomerName,
                    tgt.CustomerBk = src.CustomerBK,
                    tgt.HashKeyBK = src.HashKeyBK,
                    tgt.HashKeyValue = src.HashKeyValue,
                    tgt.InsertDate = src.InsertDate,
                    tgt.UpdateDate = src.UpdateDate
        WHEN NOT MATCHED THEN
            INSERT (CustomerBK, CustomerName, HashKeyBK, HashKeyValue, InsertDate, UpdateDate)
            VALUES (src.CustomerBK, src.CustomerName, src.HashKeyBK, src.HashKeyValue, src.InsertDate, src.UpdateDate)

        OUTPUT $action AS [Status] INTO @MergeLog;

        INSERT INTO [tmp].[changed] ([Status], Anzahl)
        SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
        GROUP BY [Status];
        """)

        conn.commit()
        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor)  
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)

   except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)