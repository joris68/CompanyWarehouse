from datetime import datetime
import logging
import pyodbc
import hashlib
import os
import json
import pytz
import traceback
from common import get_relative_blob_path, get_latest_blob_from_staging, init_DWH_Db_connection, insert_into_loadlogging, truncate_tmp_table, close_DWH_DB_connection, DWH_table_logging,insert_into_tmp_table

from azure.storage.blob import BlobServiceClient
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        # -----------------------------------------------------------
        # Load the latest json-file from the staging area
        # -----------------------------------------------------------
    
        CONTAINER = 'blueantprojectsrest'
        
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()

        data = json.loads(blob_content)

        #----------------------------------------------------------------------------------------
        # Now, retrive the data from json and pack it into variables that are loaded into DB 
        #----------------------------------------------------------------------------------------

        timezone = pytz.timezone('Europe/Berlin')
        # Get current date and time components
        now = datetime.now(timezone)
        

        projectid_list = [] # for field id in staging
        budgetvalue_list = [] # for field budgetvalue in staging
        hashkeyvalue_list = [] # for field HashkeyValue in staging
        hashkeybk_list = [] # for field HashkeyBK in staging
        datetime_list =[] # for field InsertDate and UpdateDate in staging

        for project in data['projects']:
            # we only want unique values
            projectid=project['id']
            if "customFields" in project:
                if project["customFields"]["5730249"]:
                    budgetvalue = project["customFields"]["5730249"]
                else: budgetvalue = -1
            else:
                budgetvalue = -1
            projectid_list.append(projectid)
            budgetvalue_list.append(budgetvalue)

            #hashkeybk und haskeyvalue
            bk=str(projectid)
            hash_objectbk = hashlib.sha256(bk.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hashkeybk_list.append(hex_digbk)
            # datetime_obj = datetime.strptime(date[x][:-5], "%Y-%m-%d")
            value= str(budgetvalue)
            hash_objectvalue = hashlib.sha256(value.encode())
            hex_digvalue = hash_objectvalue.hexdigest()
            hashkeyvalue_list.append(hex_digvalue)
            datetime_list.append(now)

        logging.info( "Data successfully loaded into variables.")
        
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.  
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntBudget"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntBudget"

        conn , cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        logging.info("Logging started.")

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        #load data into the temp table

        dataset = list(zip(projectid_list,budgetvalue_list,hashkeybk_list,hashkeyvalue_list,datetime_list,datetime_list))

        insert_tmp_statement = f"INSERT INTO  tmp.[dwh_FactBlueAntBudget] (ProjectBK , [BudgetValue],HashkeyBK,HashkeyValue,InsertDate,UpdateDate) VALUES (?, ?, ?, ?,?,?)"
        
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn) 

        logging.info( "Data inserted into temporary table.")

        #----------------------------------------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table.  
        #----------------------------------------------------------------------------------------

        cursor.execute(f"""
        DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

        WITH cte AS (
            SELECT 
                ISNULL(b.BlueAntProjectID, -1) AS BlueAntProjectID,
                CONVERT(DECIMAL(9, 2), a.BudgetValue) AS BudgetValue,
                a.HashkeyBK,
                CONCAT(a.HashkeyValue, '|', ISNULL(b.BlueAntProjectID, -1)) AS HashkeyValue,
                a.InsertDate,
                a.UpdateDate
            FROM tmp.dwh_FactBlueAntBudget a
            LEFT JOIN dwh.DimBlueAntProject b ON a.ProjectBK = b.ProjectBK
            WHERE a.BudgetValue != -1
        )
        MERGE dwh.FactBlueAntBudget AS tgt
        USING cte AS src
        ON (tgt.HashkeyBK = src.HashkeyBK)
        WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue THEN
            UPDATE SET 
                tgt.BlueAntProjectID = src.BlueAntProjectID,
                tgt.BudgetValue = src.BudgetValue,
                tgt.HashkeyValue = src.HashkeyValue,
                tgt.UpdateDate = src.UpdateDate
        WHEN NOT MATCHED THEN
            INSERT (BlueAntProjectID, BudgetValue, HashkeyBK, HashkeyValue, InsertDate, UpdateDate)
            VALUES (src.BlueAntProjectID, src.BudgetValue, src.HashkeyBK, src.HashkeyValue, src.InsertDate, src.UpdateDate)
        OUTPUT $action AS [Status] INTO @MergeLog;

        INSERT INTO [tmp].[changed] ([Status], Anzahl)
        SELECT [Status], COUNT(*) AS Anzahl
        FROM @MergeLog
        GROUP BY [Status];

        """)

        conn.commit()

        cursor.commit()

        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("Function was successfully executed"  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)



