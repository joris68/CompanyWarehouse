from datetime import datetime
import logging
import hashlib
import json
import traceback
from common import get_relative_blob_path, init_DWH_Db_connection, close_DWH_DB_connection, get_latest_blob_from_staging, insert_into_loadlogging, DWH_table_logging,get_time_in_string,insert_into_tmp_table,truncate_tmp_table

from azure.storage.blob import BlobServiceClient
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        CONTAINER = 'blueantprojecttask'

        now = get_time_in_string()[0]

        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())

        data = json.loads(blob.content_as_text())

        #----------------------------------------------------------------------------------------
        # Now, retrive the data from json and pack it into variables that are loaded into DB 
        #----------------------------------------------------------------------------------------

        taskid_list = [] # for field id in staging
        workActualMinutes_list = [] # for field projectbk in staging
        workActualDays_list = []
        workPlannedMinutes_list = []
        workPlannedDays_list = []
        workEstimatedMinutes_list = []
        workEstimatedDays_list = []
        hashkeyvalue_list = [] 
        hashkeybk_list = [] 
        datetime_list =[] 
        for item in data:
            for task in item['entries']:
                # we only want unique values
                taskid=task['id']
                workActualMinutes = task["workActualMinutes"]
                workActualDays = task["workActualDays"]
                workPlannedMinutes = task["workPlannedMinutes"]
                workPlannedDays = task["workPlannedDays"]
                workEstimatedMinutes = task["workEstimatedMinutes"]
                workEstimatedDays = task["workEstimatedDays"]

                taskid_list.append(taskid)
                workActualMinutes_list.append(workActualMinutes)
                workActualDays_list.append(workActualDays)
                workPlannedMinutes_list.append(workPlannedMinutes)
                workPlannedDays_list.append(workPlannedDays)
                workEstimatedMinutes_list.append(workEstimatedMinutes)
                workEstimatedDays_list.append(workEstimatedDays)
                #hashkeybk und haskeyvalue
                bk=str(taskid)
                hash_objectbk = hashlib.sha256(bk.encode())
                hex_digbk = hash_objectbk.hexdigest()
                hashkeybk_list.append(hex_digbk)
        #       datetime_obj = datetime.strptime(date[x][:-5], "%Y-%m-%d")
                value= str(workActualMinutes)+str(workActualDays)+str(workPlannedMinutes)+str(workPlannedDays)+str(workEstimatedMinutes)+str(workEstimatedDays)
                hash_objectvalue = hashlib.sha256(value.encode())
                hex_digvalue = hash_objectvalue.hexdigest()
                hashkeyvalue_list.append(hex_digvalue)
                datetime_list.append(now)

        logging.info("Data successfully loaded into variables.")
        conn, cursor = init_DWH_Db_connection()

        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.  
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntProjectTask"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntProjectTask"

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        logging.info("Truncated Temp Table")

        # now inserting new values into the temp table

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        #load data into the temp table

        dataset = list(zip(taskid_list,workActualMinutes_list,workActualDays_list,workPlannedMinutes_list,workPlannedDays_list,workEstimatedMinutes_list,workEstimatedDays_list,hashkeybk_list,hashkeyvalue_list,datetime_list,datetime_list))

        insert_tmp_statement = f"INSERT INTO  tmp.[dwh_FactBlueAntProjectTask] (BlueAntTaskBK , [WorkActualMinutes],[WorkActualDays],[WorkPlannedMinutes],[WorkPlannedDays],[WorkEstimatedMinutes],[WorkEstimatedDays],HashkeyBK,HashkeyValue,InsertDate,UpdateDate) VALUES (?, ?, ?, ?,?,?,?,?,?,?,?)"

        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn) 
        
        # Commit the transaction

        logging.info("Data inserted into temporary table.")

        #----------------------------------------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table.  
        #----------------------------------------------------------------------------------------

        
        cursor.execute(f"""
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));
            with cte as(
                Select 
                    ISNULL(b.BlueAntProjectTaskID,-1) as BlueAntProjectTaskID
                    ,WorkActualMinutes
                    ,WorkActualDays
                    ,WorkPlannedMinutes
                    ,WorkPlannedDays
                    ,WorkEstimatedMinutes
                    ,WorkEstimatedDays
                    ,a.HashkeyBK
                    ,CONCAT(a.HashkeyValue,'|',ISNULL(b.BlueAntProjectTaskID,-1)) as HashkeyValue
                    ,a.InsertDate
                    ,a.UpdateDate
                from tmp.dwh_FactBlueAntProjectTask a
                LEFT JOIN dwh.DimBlueAntProjectTask b
                on a.BlueAntTaskBK = b.BlueAntTaskBK
                )
                Merge dwh.FactBlueAntProjectTask as tgt
                using cte as src
                ON (tgt.HashkeyBK = src.HashkeyBK)
                WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue
                then update set 
                    tgt.BlueAntProjectTaskID = src.BlueAntProjectTaskID,
                    tgt.WorkActualMinutes = src.WorkActualMinutes,
                    tgt.WorkActualDays = src.WorkActualDays,
                    tgt.WorkPlannedMinutes = src.WorkPlannedMinutes,
                    tgt.WorkPlannedDays = src.WorkPlannedDays,
                    tgt.WorkEstimatedMinutes = src.WorkEstimatedMinutes,
                    tgt.WorkEstimatedDays = src.WorkEstimatedDays,
                    tgt.HashkeyValue = src.HashkeyValue,
                    tgt.UpdateDate = src.UpdateDate
                    when not matched then insert
                (BlueAntProjectTaskID,WorkActualMinutes, WorkActualDays  ,WorkPlannedMinutes,WorkPlannedDays,WorkEstimatedMinutes,WorkEstimatedDays,HashkeyBK, HashkeyValue, InsertDate, UpdateDate)
                Values(src.BlueAntProjectTaskID, src.WorkActualMinutes, src.WorkActualDays ,
                        src.WorkPlannedMinutes, src.WorkPlannedDays,  src.WorkEstimatedMinutes, src.WorkEstimatedDays,
                            src.HashkeyBK, src.HashkeyValue, src.InsertDate, src.UpdateDate )

                OUTPUT  $action as [Status] into @MergeLog;
                INSERT INTO [tmp].[changed] ([Status],Anzahl)
                SELECT [Status], count(*) as Anzahl FROM @MergeLog
                GROUP BY [Status];
        """)

        conn.commit()

        cursor.commit()

        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        # Close the database connection
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)



