#Author: Pascal Schütze 
import pandas as pd
import hashlib
import numpy as np
from common import get_latest_blob_from_staging, truncate_tmp_table, lookup_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string
import traceback 
import io
import os
from azure.storage.blob import BlobServiceClient

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:

        # -----------------------------------------------------------
        # Load the latest xlsx-file from the dataverse datalake
        # -----------------------------------------------------------
    
        CONTAINER = 'dataverse-ceterisag-org84334ab9'
    
        relative_path = '/cet_leadsundverkaufschancen/'
        
        connection_string = os.environ['DataverseConnectionString']
            
        # Build a connection to the Blob Storage    
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # Get the container client based on the container name
        container_client = blob_service_client.get_container_client(CONTAINER)
        
        # List all blobs in the container and find the latest one based on last_modified time.
        # Note: Folder are in Datalakes not real folders, they are prefixes in the blob name

        blob_list = container_client.list_blobs(name_starts_with=relative_path+"20")

        dfs = []

        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob)

            blob_content = blob_client.download_blob().readall()

            data_stream = io.BytesIO(blob_content)

            data_forLoop = pd.read_csv(data_stream, sep=',', encoding='utf-8', header=None, engine='python', usecols=range(110))
            data_forLoop = data_forLoop.iloc[:, [3, 0, 64, 59, 14, 65, 102]]
            
            dfs.append(data_forLoop)
            
        combined_df = pd.concat(dfs,ignore_index=True)

        combined_df.columns = ['Status','DealBK','DealName','CustomerName','DealStateID','DealPlannedDays','LastChangedDateID']
        
        data = combined_df
        
        # Filter the data to only include the rows with Status = 0 for active deals
        data = data[data['Status'] == 0]
        data = data.drop(columns=['Status'])
        
        # Replace all NaN values with 0
        data['DealPlannedDays'] = data['DealPlannedDays'].fillna(0)
        data['DealStateID'] = data['DealStateID'].fillna(-1)
        data['CustomerName'] = data['CustomerName'].fillna('N/A')
        
        # Convert the column to a datetime object
        data['LastChangedDateID'] = pd.to_datetime(data['LastChangedDateID'])

        # Format the datetime object to your desired format
        data['TimeStamp'] = data['LastChangedDateID'].dt.strftime('%Y%m%d%H%M%S')
        data['TimeStamp'] = data['LastChangedDateID'].astype(np.int64)
        
        # Get the indices of rows with the maximum LastChangedDateID for each DealBK
        idx = data.groupby('DealBK')['TimeStamp'].idxmax()
        
        # Subset the dataframe using these indices
        data = data.loc[idx]
        
        #Reduce the LastChangedDateID to only the date
        data['LastChangedDateID'] = data['LastChangedDateID'].dt.strftime('%Y%m%d')
        data = data.drop(columns=['TimeStamp']) 
        
        #Create the Hashkeys
        data['HashkeyBK'] = data.apply(lambda row: hashlib.sha256(str(row['DealBK']).encode('utf-8')).hexdigest(), axis = 1)
        data['HashkeyValue'] = data.apply(lambda row: hashlib.sha256((str(row['DealName']) + str(row['CustomerName']) + str(row['DealStateID']) + str(row['DealPlannedDays']) + str(row['LastChangedDateID'])).encode('utf-8')).hexdigest(), axis=1)
        
        dataset = list(zip(data['DealBK'], data['DealName'], data['CustomerName'], data['DealStateID'], data['DealPlannedDays'], data['LastChangedDateID'], data['HashkeyBK'], data['HashkeyValue']))
        
        #----------------------------------------------------------------------------------------
        # Now, update the Log Table for DimDeal and insert the data into tmp table. 
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimDeal"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimDeal"
        
        conn, cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)
        
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        
        InsertQuery = f""" INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] 
                        (DealBK, DealName, CustomerName, DealStateID, DealPlannedDays, LastChangedDateID, InsertDate, UpdateDate, HashkeyBK, HashkeyValue)
                    VALUES
                        (?, ?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?)"""
                        
        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)
        
        LookUpQuery = f"""
                WITH CTE AS (
                    SELECT 
                        d.DealBK,
                        d.DealName,
                        d.CustomerName,
                        ISNULL(ds.DealStateID, -1) as DealStateID,
                        d.DealPlannedDays,
                        d.LastChangedDateID,
                        d.HashkeyBK,
                        d.HashkeyValue AS OldHashkeyValue,  -- Renaming for clarity
                        LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                                        CONCAT(CONVERT(VARCHAR, d.DealName), 
                                                CONVERT(VARCHAR, d.CustomerName),
                                                CONVERT(VARCHAR, ds.DealStateID),
                                                CONVERT(VARCHAR, d.DealPlannedDays),
                                                CONVERT(VARCHAR, d.LastChangedDateID))), 2)) AS NewHashkeyValue  -- New hashkey generation because of the LookUp 
                                                
                FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] d
                LEFT JOIN dwh.DimDealState ds ON d.DealStateID = ds.DealStateBK

            )
            UPDATE tmp
            SET
                tmp.DealStateID = c.DealStateID,
                tmp.HashkeyValue = c.NewHashKeyValue  -- Using the new hashkey
                        
            FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp
            JOIN CTE c ON tmp.HashkeyBK = c.HashkeyBK;
        """
        
        lookup_tmp_table(LookUpQuery, cursor, conn)
        
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
            TARGET.[DealName] = SOURCE.[DealName],
            TARGET.[CustomerName] = SOURCE.[CustomerName],
            TARGET.[DealStateID] = SOURCE.[DealStateID],
            TARGET.[DealPlannedDays] = SOURCE.[DealPlannedDays],
            TARGET.[LastChangedDateID] = SOURCE.[LastChangedDateID],
            TARGET.[UpdateDate] = SOURCE.[UpdateDate]
        WHEN NOT MATCHED BY TARGET 
        THEN INSERT ([DealBK], [DealName], [CustomerName], [DealStateID], [DealPlannedDays], [LastChangedDateID], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
        VALUES (SOURCE.[DealBK], SOURCE.[DealName], SOURCE.[CustomerName], SOURCE.[DealStateID], SOURCE.[DealPlannedDays], SOURCE.[LastChangedDateID], SOURCE.[InsertDate], SOURCE.[UpdateDate], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue])
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