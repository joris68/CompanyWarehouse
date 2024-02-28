#Changes: PSC
import logging
import hashlib
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string,get_blob_start_with
import traceback




def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
    
        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area "blueantprojectservice"
        # -----------------------------------------------------------

        CONTAINER = 'initialloads'

        now = get_time_in_string()[0]
    
        blob_content = get_blob_start_with(CONTAINER,'blueantprojecttask').content_as_text()  

        # -----------------------------------------------------------
        # Now, retrive the data from json and pack it into variables 
        # -----------------------------------------------------------

        # Load the JSON data into a dictionary
        data = json.loads(blob_content)

        TaskIDs = [] 
        DescriptionNames = []
        ProjectBKs = [] 
        TaskParents = []
        datetime_list = []

        for item in data:
            projectbk = item["projectbk"]
            
            for task in item['entries']:
                
                # we only want unique values
                taskid=task['id']
                description = task["description"]

                # check if task has a parent
                if "parentId" in task:
                    taskparent = task["parentId"]
                else:
                    taskparent = -1

                TaskIDs.append(taskid)
                DescriptionNames.append(description)
                ProjectBKs.append(projectbk)
                TaskParents.append(taskparent)
                datetime_list.append(now)
                
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------
        
        hashkeyBK_list = [hashlib.sha256(str(TaskIDs[x]).encode()).hexdigest() for x in range(0, len(TaskIDs))]

        hashkeyValue_list = [hashlib.sha256((str(DescriptionNames[x]) + str(ProjectBKs[x])).encode()).hexdigest() for x in range(0, len(DescriptionNames))]

        dataset = list(zip(TaskIDs, ProjectBKs, DescriptionNames, TaskParents, hashkeyBK_list, hashkeyValue_list,datetime_list,datetime_list))
        
        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------
    
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntProjectTask"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntProjectTask"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        InsertQuery = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (BlueAntTaskBK, ProjectBK, TaskName, TaskParent, HashkeyBK, HashkeyValue, InsertDate, UpdateDate) VALUES (?,?,?,?,?,?, ?,?)"

        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)

        # -----------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table.  
        # -----------------------------------------------------------

        cursor.execute(f"""
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));

            WITH CTE AS (
                SELECT
                    ISNULL(b.BlueAntProjectID, -1) AS BlueAntProjectID,
                    TaskName,
                    BlueAntTaskBK,
                    a.HashkeyBK,
                    a.TaskParent,
                    CONCAT(a.HashkeyValue, '|', ISNULL(b.BlueAntProjectID, -1)) AS HashkeyValue,
                    a.InsertDate,
                    a.UpdateDate
                FROM 
                    [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] a
                    LEFT JOIN dwh.DimBlueAntProject b ON a.ProjectBK = b.ProjectBK
            )

            MERGE  [{SCHEMA_NAME}].[{TABLE_NAME}] AS tgt
            USING CTE AS src
            ON tgt.HashkeyBK = src.HashkeyBK
            WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue THEN
                UPDATE SET
                    tgt.BlueAntProjectID = src.BlueAntProjectID,
                    tgt.TaskName = src.TaskName,
                    tgt.BlueAntTaskBK = src.BlueAntTaskBK,
                    tgt.HashkeyValue = src.HashkeyValue,
                    tgt.UpdateDate = src.UpdateDate,
                    tgt.TaskParent = src.TaskParent
            WHEN NOT MATCHED THEN
                INSERT (BlueAntTaskBK, BlueAntProjectID, TaskName, TaskParent, HashkeyBK, HashkeyValue, InsertDate, UpdateDate)
                VALUES (src.BlueAntTaskBK, src.BlueAntProjectID, src.TaskName, src.TaskParent, src.HashkeyBK, src.HashkeyValue, src.InsertDate, src.UpdateDate)
            OUTPUT $action AS [Status] INTO @MergeLog;

            INSERT INTO tmp.[changed] ([Status], Anzahl)
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