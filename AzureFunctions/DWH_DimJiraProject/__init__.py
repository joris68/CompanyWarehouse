#Autor:MC, Changed: PSC
import logging
import azure.functions as func
import json
import hashlib
from common import get_latest_blob_from_staging, get_relative_blob_path, create_file_name, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string,truncate_tmp_table,insert_into_tmp_table
import traceback



def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:

        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area "blueantprojectservice"
        # -----------------------------------------------------------
    
        CONTAINER = 'jiraproject'

        now = get_time_in_string()[0]
    
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()  

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

        # Load the json file
        data = json.loads(blob_content)
        
        # Variables for the tmp table
        project_names = []
        project_ids = []
        project_keys = []
        projectcategory_ids = []

        datetime_list =[]

        values = data["values"]

        for items in values:
            if items["id"] not in project_ids:
                project_names.append(items["name"])
                project_ids.append(items["id"])
                project_keys.append(items["key"])
                if 'projectCategory' in items:
                    projectcategory_ids.append(items["projectCategory"]["id"])
                else:
                    projectcategory_ids.append(-1)
                datetime_list.append(now)
    
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------

        hashkeyBK_list = [hashlib.sha256(str(project_ids[x]).encode()).hexdigest() for x in range(len(project_ids))]
        
        hashkeyValue_list = [hashlib.sha256(str(project_names[x] + str(project_keys[x]) + str(projectcategory_ids[x])).encode()).hexdigest() for x in range(len(project_names))]

        dataset = list(zip(project_ids, projectcategory_ids, project_keys, project_names,datetime_list,datetime_list, hashkeyBK_list, hashkeyValue_list))
    
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.  
        #----------------------------------------------------------------------------------------

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimJiraProject"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimJiraProject"
    
        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        InsertQuery = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (JiraProjectBK, JiraProjectCategoryID, JiraProjectShortName, JiraProjectName, InsertDate, UpdateDate, HashkeyBK, HashkeyValue) VALUES (?,?,?,?,?,?,?,?)"

        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)

        cursor.execute(f"""
            DECLARE @MergeLog TABLE ([Status] VARCHAR(20));
            
            WITH CTE AS (
                SELECT tmp.[JiraProjectBK], dwh_pc.[ProjectCategoryID] as JiraProjectCategoryID, tmp.[JiraProjectShortName], tmp.[JiraProjectName], tmp.[InsertDate], tmp.[UpdateDate], tmp.[HashkeyBK], tmp.[HashkeyValue]
                FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp
                LEFT JOIN dwh.DimJiraProjectCategory dwh_pc ON tmp.[JiraProjectCategoryID] = dwh_pc.ProjectCategoryBK
            )
            
            MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
            USING CTE AS SOURCE
            ON (TARGET.[HashkeyBK] = SOURCE.[HashkeyBK])
            WHEN MATCHED AND TARGET.[HashkeyValue] <> SOURCE.[HashkeyValue] 
            THEN UPDATE SET TARGET.[HashkeyValue] = SOURCE.[HashkeyValue], 
                            TARGET.[JiraProjectBK] = SOURCE.[JiraProjectBK], 
                            TARGET.[JiraProjectCategoryID] = SOURCE.[JiraProjectCategoryID], 
                            TARGET.[JiraProjectShortName] = SOURCE.[JiraProjectShortName], 
                            TARGET.[JiraProjectName] = SOURCE.[JiraProjectName], 
                            TARGET.[UpdateDate] = SOURCE.[UpdateDate]
            WHEN NOT MATCHED BY TARGET 
            THEN INSERT ([JiraProjectBK], [JiraProjectCategoryID], [JiraProjectShortName], [JiraProjectName], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
            VALUES (SOURCE.[JiraProjectBK], SOURCE.[JiraProjectCategoryID], SOURCE.[JiraProjectShortName], SOURCE.[JiraProjectName], SOURCE.[InsertDate], SOURCE.[UpdateDate], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue])
            OUTPUT $action AS [Status] INTO @MergeLog;

            INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status], Anzahl)
            SELECT [Status], COUNT(*) AS Anzahl 
            FROM @MergeLog
            GROUP BY [Status];
            
            """)
        
        # Commit transaction and log success
        conn.commit()

        cursor.commit()

        logging.info("The Merge statement was successfully executed.")

        #----------------------------------------------------------------------------------------
        # Now, update the Log Table for DimJiraProject. 
        #----------------------------------------------------------------------------------------

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor) 
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)