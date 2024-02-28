#Author: Pascal Schütze 
import pandas as pd
import hashlib
from common import get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string
import traceback 

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:

        # -----------------------------------------------------------
        # Load the latest xlsx-file from the dataverse datalake
        # -----------------------------------------------------------
    
        CONTAINER = 'dataverse-ceterisag-org84334ab9'
    
        relative_path = '/cet_dimdealstate/'
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path, datalake_connection="dataverse").readall()
        
        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

        # Load the JSON data into a dictionary
        data = pd.DataFrame(pd.read_excel(blob_content, sheet_name=0, skiprows=1, header=None, engine='openpyxl'))
        
        # Rename the columns
        data.columns = ['DealStateBK', 'DealStateName', 'DealStatePipeline']
     
        data['HashkeyBK'] = data.apply(lambda row: hashlib.sha256(str(row['DealStateBK']).encode('utf-8')).hexdigest(), axis = 1)
        data['HashkeyValue'] = data.apply(lambda row: hashlib.sha256((str(row['DealStateName']) + str(row['DealStatePipeline'])).encode('utf-8')).hexdigest(), axis=1)

        dataset = list(zip(data['DealStateBK'], data['DealStateName'], data['DealStatePipeline'], data['HashkeyBK'], data['HashkeyValue']))
        
        #----------------------------------------------------------------------------------------
        # Now, update the Log Table for DimDealState and insert the data into tmp table. 
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimDealState"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimDealState"
        
        conn, cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)
        
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        
        InsertQuery = f""" INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] 
                        (DealStateBK, DealStateName, DealStatePipeline, InsertDate, UpdateDate, HashkeyBK, HashkeyValue)
                    VALUES
                        (?, ?, ?, GETDATE(), GETDATE(), ?, ?)"""
                        
        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)
        
        #----------------------------------------------------------------------------------------
        # Now, merge the tmp table into the DWH table
        #----------------------------------------------------------------------------------------
        
        cursor.execute(f"""
        DECLARE @MergeLog TABLE([Status] VARCHAR(20));

        -- MERGE statement
        MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
        USING [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS SOURCE
        ON (TARGET.[HashkeyBK] = SOURCE.[HashkeyBK])
        WHEN MATCHED AND TARGET.[HashkeyValue] <> SOURCE.[HashkeyValue]
        THEN UPDATE SET TARGET.[HashkeyValue] = SOURCE.[HashkeyValue],
            TARGET.[DealStateName] = SOURCE.[DealStateName],
            TARGET.[DealStatePipeline] = SOURCE.[DealStatePipeline],
            TARGET.[UpdateDate] = SOURCE.[UpdateDate]
        WHEN NOT MATCHED BY TARGET 
        THEN INSERT ([DealStateBK], [DealStateName], [DealStatePipeline], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
        VALUES (SOURCE.[DealStateBK], SOURCE.[DealStateName], SOURCE.[DealStatePipeline],  SOURCE.[InsertDate], SOURCE.[UpdateDate], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue])
        OUTPUT  $action as [Status] into @MergeLog;

        INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status],Anzahl)
        SELECT [Status], count(*) as Anzahl FROM @MergeLog
        GROUP BY [Status];

        """)

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension= True  )

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
     # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)