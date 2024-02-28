#Changes: PSC
from datetime import datetime
import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pyodbc
from xml.etree import ElementTree as ET
import hashlib
import pytz
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string
import traceback



def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:
    
        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area "blueantprojectservice"
        # -----------------------------------------------------------
    
        CONTAINER = 'blueantprojectroles'

        now = get_time_in_string()[0]
    
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()  

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------
  
        xml_content = ET.fromstring(blob_content)

        ProjectRoleIDs = []
        ProjectRoleNames = []
        ProjectRoleExternals = []
        ProjectRoleTravels = []
        datetime_list = []

        # Namespace-Deklarationen
        ns = {'ns1': 'http://project.blueant.axis.proventis.net/'}

        # Alle <ns1:ProjectRole>-Elemente auswählen
        project_roles = xml_content.findall('.//ns1:ProjectRole', namespaces=ns)


        # Informationen aus den Elementen extrahieren
        for project_role in project_roles:
            
            project_role_id = project_role.find('ns1:projectRoleID', namespaces=ns).text
            name = project_role.find('ns1:name', namespaces=ns).text
            external = project_role.find('ns1:external', namespaces=ns).text
            travel = project_role.find('ns1:travel', namespaces=ns).text
            
            ProjectRoleIDs.append(project_role_id)
            ProjectRoleNames.append(name)
            ProjectRoleExternals.append(external)
            ProjectRoleTravels.append(travel)
            datetime_list.append(now)
            
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset 
        # -----------------------------------------------------------

        #HashkeyBK wird aus project_role_id gebildet
        hashkeyBK_list = [hashlib.sha256(str(ProjectRoleIDs[x]).encode()).hexdigest() for x in range(0, len(ProjectRoleIDs))]
        
        #HashkeyValue wird aus allen anderen Attributen gebildet
        hashkeyValue_list = [hashlib.sha256((str(ProjectRoleNames[x]) + str(ProjectRoleExternals[x]) + str(ProjectRoleTravels[x])).encode()).hexdigest() for x in range(0, len(ProjectRoleNames))]  

        dataset = list(zip(ProjectRoleIDs, ProjectRoleNames, ProjectRoleExternals, ProjectRoleTravels, hashkeyBK_list, hashkeyValue_list,datetime_list,datetime_list))

        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntProjectRoles"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntProjectRoles"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        insert_tmp_statement = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (ProjectRoleBK, ProjectRoleName, ExternalCost, TravelCost, HashkeyBK, HashkeyValue, InsertDate, UpdateDate) VALUES (?,?,?,?,?,?, ?,?)"

        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        cursor.execute(f"""
            -- Synchronize the target table with refreshed data from the source table
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));

            WITH CTE AS (
                SELECT
                    CONVERT(INT, ProjectRoleBK) AS ProjectRoleBK,
                    ProjectRoleName,
                    CONVERT(DECIMAL(9, 4), ExternalCost) AS ExternalCost,
                    CONVERT(DECIMAL(9, 4), TravelCost) AS TravelCost,
                    HashkeyBK,
                    HashkeyValue,
                    InsertDate,
                    UpdateDate
                FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
            )

            MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
            USING CTE AS SOURCE
            ON TARGET.HashkeyBK = SOURCE.HashkeyBK
            WHEN MATCHED AND TARGET.HashkeyValue <> SOURCE.HashkeyValue THEN
                UPDATE SET
                    TARGET.HashkeyValue = SOURCE.HashkeyValue,
                    TARGET.ProjectRoleName = SOURCE.ProjectRoleName,
                    TARGET.[ExternalCost] = SOURCE.[ExternalCost],
                    TARGET.[TravelCost] = SOURCE.[TravelCost],
                    TARGET.UpdateDate = SOURCE.UpdateDate
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ([ProjectRoleBK], ProjectRoleName, ExternalCost, [TravelCost], HashkeyBK, HashkeyValue, InsertDate, UpdateDate) 
                VALUES (SOURCE.ProjectRoleBK, SOURCE.ProjectRoleName, SOURCE.ExternalCost, SOURCE.[TravelCost], SOURCE.HashkeyBK, SOURCE.HashkeyValue, SOURCE.InsertDate, SOURCE.UpdateDate)
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