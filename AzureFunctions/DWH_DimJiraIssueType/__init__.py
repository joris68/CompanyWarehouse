import logging
import json
import hashlib
import azure.functions as func

from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:

        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area "blueantprojectservice"
        # -----------------------------------------------------------

        CONTAINER = 'jiraissuekeys'

        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()  

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------
   
        data = json.loads(blob_content)
        
        distinct_issueTypenames = []
        distinct_issueTypeIDs = []
        
        for object in data:
            name = object['fields']['issuetype']['name']
            id = object['fields']['issuetype']['id']
        
            if name not in distinct_issueTypenames:
                distinct_issueTypenames.append(name)
            
            if id not in distinct_issueTypeIDs:
                distinct_issueTypeIDs.append(id)
                
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------
        
        hashkeyBK_list = [hashlib.sha256(str(distinct_issueTypeIDs[x]).encode()).hexdigest() for x in range(len(distinct_issueTypeIDs))]

        hashkeyValue_list = [hashlib.sha256(str(distinct_issueTypenames[x]).encode()).hexdigest() for x in range(len(distinct_issueTypenames))]
        
        dataset = list(zip(distinct_issueTypeIDs, distinct_issueTypenames, hashkeyBK_list, hashkeyValue_list))
        
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.  
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimJiraIssueType"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimJiraIssueType"
        
        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        InsertQuery = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] ( IssueTypeId ,IssueTypeName, InsertDate, UpdateDate, HashkeyBk, HashkeyValue) VALUES (?,?,getDate(),getDate(),?,?)"

        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)

        cursor.execute(f"""
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));

            MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS tgt
            USING [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS src
            ON tgt.HashkeyBK = src.HashkeyBK

            WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue THEN
                UPDATE SET tgt.HashkeyValue = src.HashkeyValue,
                        tgt.HashkeyBK = src.HashkeyBK,
                        tgt.JiraIssueType = src.IssueTypeName,
                        tgt.JiraIssueTypeBK = src.IssueTypeId

            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([JiraIssueTypeBK], [JiraIssueType], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
                VALUES (src.IssueTypeId, src.IssueTypeName, src.InsertDate, src.UpdateDate, src.HashkeyBk, src.HashkeyValue)

            OUTPUT $action AS [Status] INTO @MergeLog;

            INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status], Anzahl)
            SELECT [Status], COUNT(*) AS Anzahl
            FROM @MergeLog
            GROUP BY [Status];""")

        # Commit transaction and log success
        conn.commit()
        logging.info("The Merge statement was successfully executed.")

        #----------------------------------------------------------------------------------------
        # Now, update the Log Table for DimJiraIssue. 
        #----------------------------------------------------------------------------------------

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor) 
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
