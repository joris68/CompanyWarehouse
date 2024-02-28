#Changes: PSC
import logging
import hashlib
from xml.etree import ElementTree as ET
import json
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string
import azure.functions as func
import traceback
import pandas as pd 

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
                            
        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area "blueantprojectsrest"
        # -----------------------------------------------------------
    
        CONTAINER_PROJECTS_REST = 'blueantprojectsrest'

        now = get_time_in_string()[0]
    
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER_PROJECTS_REST, folder_name=relative_path).content_as_text()  

        # Load the JSON data into a dictionaryl
        data = json.loads(blob_content)

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------
        
        project_IDs = []
        customer_IDs = []
        projectnames = []
        startTimes = []
        endTimes = []
        projectLeaderIDs = []
        projectNumbers = []
        projectStateIDS = []
        datetime_list=[]
         
        for project in data['projects']:
            projectid=project['id']

            if projectid not in project_IDs:
                projectname=project["name"]
                projectstarttime = project["start"]
                projectendtime = project["end"]
                customerid = project["clients"][0]["clientId"]
                projectleader=project["projectLeaderId"]
                projectnumber=project["number"]
                projectstate = project["statusId"]
                

                project_IDs.append(projectid)
                projectnames.append(projectname)
                customer_IDs.append(customerid)
                startTimes.append(str(projectstarttime[0:10]))
                endTimes.append(str(projectendtime[0:10]))
                projectLeaderIDs.append(projectleader)
                projectNumbers.append(projectnumber)
                projectStateIDS.append(projectstate)
                datetime_list.append(now)
                
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------
        
        hashkeyValue_list = [hashlib.sha256((str(customer_IDs[x]) + str(projectnames[x]) + str(startTimes[x]) + str(endTimes[x]) + str(projectLeaderIDs[x]) + str(projectNumbers[x]) + str(projectStateIDS)).encode()).hexdigest() for x in range(0, len(project_IDs))]
        
        hashkeyBK_list = [hashlib.sha256(str(project_IDs[x]).encode()).hexdigest() for x in range(0, len(project_IDs))]
        
        dataset = list(zip(projectNumbers , projectLeaderIDs, customer_IDs, project_IDs, projectnames, startTimes, endTimes, projectStateIDS,datetime_list,datetime_list, hashkeyBK_list, hashkeyValue_list))
        
        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntProject"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntProject"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        
        insert_tmp_statement = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]  ([ProjectNumber], [ProjectLeaderID], [CustomerBK], [ProjectBK], [ProjectName], [StartDate], [EndDate], [ProjectStateBK], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue]) VALUES (?,?,?,?,?,?,?,?, ?,?, ?,?)"
         
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        # -----------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table.  
        # -----------------------------------------------------------

        cursor.execute(f"""
            DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

            WITH CTE AS (
                SELECT 
                    [ProjectNumber],
                    ISNULL(u.[UserID], -1) AS ProjectManagerID,
                    ISNULL(c.[BlueAntCustomerID], -1) AS CustomerID,
                    [ProjectBK],
                    [ProjectName],
                    CAST(CONVERT(varchar(10), [StartDate], 112) AS INT) AS [StartDate],
                    CAST(CONVERT(varchar(10), [EndDate], 112) AS INT) AS [EndDate],
                    ISNULL(s.ProjectStateID,-1) as ProjectStateID,
                    p.[InsertDate],
                    p.[UpdateDate],
                    p.[HashkeyBK],
                    CONCAT(p.HashkeyValue, '|', ISNULL(u.[UserID], -1), '|', ISNULL(c.[BlueAntCustomerID], -1)) AS HashkeyValue
                FROM
                    [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS p
                    LEFT JOIN dwh.DimBlueAntCustomer AS c   ON p.CustomerBK = c.CustomerBK
                    LEFT JOIN dwh.DimUser AS u              ON p.ProjectLeaderID = u.BlueAntUserBK
                    LEFT JOIN dwh.DimBlueAntProjectState s  ON p.ProjectStateBK = s.ProjectStateBK
            )
            
            MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS tgt
            USING CTE AS src
            ON tgt.ProjectBK = src.ProjectBK
            WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue THEN
                UPDATE SET 
                    tgt.[ProjectNumber] = src.[ProjectNumber],
                    tgt.[ProjectManagerID] = src.[ProjectManagerID],
                    tgt.[CustomerID] = src.[CustomerID],
                    tgt.[ProjectBK] = src.[ProjectBK],
                    tgt.[ProjectName] = src.[ProjectName],
                    tgt.[ProjectStateID] = src.[ProjectStateID],
                    tgt.[StartDateID] = src.[StartDate],
                    tgt.[EndDateID] = src.[EndDate],
                    tgt.InsertDate = src.InsertDate,
                    tgt.UpdateDate = src.UpdateDate,
                    tgt.HashKeyBK = src.HashKeyBK,
                    tgt.HashkeyValue = src.HashkeyValue
            WHEN NOT MATCHED THEN
                INSERT ([ProjectNumber], [ProjectManagerID], [CustomerID], [ProjectBK], [ProjectName], [ProjectStateID],[StartDateID], [EndDateID], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
                VALUES (src.[ProjectNumber], src.[ProjectManagerID], src.[CustomerID], src.[ProjectBK], src.[ProjectName], src.[ProjectStateID], src.[StartDate], src.[EndDate], src.InsertDate, src.UpdateDate, src.HashkeyBK, src.HashkeyValue)
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