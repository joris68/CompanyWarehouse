#Changes: PSC
import logging
from datetime import datetime
import hashlib
from xml.etree import ElementTree as ET

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:
   
    try:
    
        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area
        # -----------------------------------------------------------
    
        CONTAINER = 'blueantinvoicestate'
    
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()  

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

        root = ET.fromstring(blob_content)

        # Find all 'stateName' and 'invoiceStateID' elements
        state_names = root.findall(".//{http://masterdata.blueant.axis.proventis.net/}stateName")
        invoice_state_ids = root.findall(".//{http://masterdata.blueant.axis.proventis.net/}invoiceStateID")

        # Extract the values of 'stateName' elements
        state_name_values = [state_name.text for state_name in state_names]

        # Extract the values of 'invoiceStateID' elements
        invoice_state_id_values = [invoice_state_id.text for invoice_state_id in invoice_state_ids]

        hashkeyBK_list = [hashlib.sha256(str(invoice_state_id).encode()).hexdigest() for invoice_state_id in invoice_state_id_values]

        hashkeyValue_list = [hashlib.sha256(str(state_name).encode()).hexdigest() for state_name in state_name_values]
        
        dataset = list(zip(invoice_state_id_values, state_name_values, hashkeyBK_list, hashkeyValue_list))

        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntInvoiceState"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntInvoiceState"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        
        insert_tmp_statement = f"""
            INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] 
            (
                [InvoiceStateBK]
                ,[InvoiceStateName]
                ,[HashkeyBK]
                ,[HashkeyValue]
                ,[InsertDate]
                ,[UpdateDate]
            ) VALUES 
            (
                ?
                ,?
                ,?
                ,?
                ,getDate()
                ,getDate()
            )"""
        
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        cursor.execute(f"""
        DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

        MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS tgt
        USING [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS src
            ON (tgt.HashkeyBK = src.HashkeyBK)
        WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue 
        THEN UPDATE SET tgt.HashKeyValue = src.HashKeyValue,
                        tgt.InvoiceStateBK = src.InvoiceStateBK,
                        tgt.InvoiceStateName = src.InvoiceStateName,
                        tgt.UpdateDate = src.UpdateDate
        WHEN NOT MATCHED THEN
            INSERT (
                        InvoiceStateBK
                        , InvoiceStateName
                        , InsertDate
                        , UpdateDate
                        , HashKeyBK
                        , HashKeyValue
                    )
            VALUES (
                        src.InvoiceStateBK
                        , src.InvoiceStateName
                        , src.InsertDate
                        , src.UpdateDate
                        , src.HashKeyBK
                        , src.HashKeyValue
                    )
                    
        OUTPUT $action AS [Status] INTO @MergeLog;

        INSERT INTO [tmp].[changed] ([Status], Anzahl)
        SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
        GROUP BY [Status];
        """)
        
        conn.commit()
        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=True)

        close_DWH_DB_connection(conn, cursor)  
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)