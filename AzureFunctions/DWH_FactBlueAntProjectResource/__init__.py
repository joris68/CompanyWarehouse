from datetime import datetime
import logging
import azure.functions as func
from xml.etree import ElementTree as ET
import hashlib
import pytz
from common import get_relative_blob_path, init_DWH_Db_connection, close_DWH_DB_connection, get_latest_blob_from_staging, insert_into_loadlogging, DWH_table_logging,get_time_in_string,insert_into_tmp_table,truncate_tmp_table
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
 
        timezone = pytz.timezone('Europe/Berlin')
        # Get current date and time components
        now = datetime.now(timezone)

        CONTAINER = 'blueantprojectresource'

        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())

        blob_content = blob.content_as_text()
    
        xml_content = ET.fromstring(blob_content)

        ResourceBK_list =[]
        RoleBK_list =[]
        personBK_list =[]
        projectBK_list =[]
        hashkeyvalue_list = []
        hashkeybk_list = []
        datetime_list =[]

        ns = {'ns1': 'http://project.blueant.axis.proventis.net/'}

        project_resources = xml_content.findall('.//ns1:ProjectResource', namespaces=ns)

        for project_resource in project_resources:
            resource_id = project_resource.find('ns1:resourceID', namespaces=ns).text
            role_id = project_resource.find('ns1:roleID', namespaces=ns).text
            person_id = project_resource.find('ns1:personID', namespaces=ns).text
            project_id = project_resource.find('ns1:projectid', namespaces=ns).text

            ResourceBK_list.append(resource_id)
            RoleBK_list.append(role_id)
            personBK_list.append(person_id)
            projectBK_list.append(project_id)

            #hashkeybk und haskeyvalue
            bk=resource_id
            hash_objectbk = hashlib.sha256(bk.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hashkeybk_list.append(hex_digbk)

            value= (role_id)+ str(person_id)+ str(project_id)
            hash_objectvalue = hashlib.sha256(value.encode())
            hex_digvalue = hash_objectvalue.hexdigest()
            hashkeyvalue_list.append(hex_digvalue)

            datetime_list.append(now)

        dataset = list(zip(ResourceBK_list,RoleBK_list,personBK_list,projectBK_list,hashkeybk_list,hashkeyvalue_list,datetime_list,datetime_list))

        conn, cursor = init_DWH_Db_connection()

        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntProjectResource"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntProjectResource"

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        insert_tmp_statement = f"INSERT INTO  {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} (ResourceID , ProjectRoleBK ,PersonBK ,ProjectBK,HashkeyBK,HashkeyValue,InsertDate,UpdateDate) VALUES (?, ?,?,?,?,?,?,?)"
        
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        try: 
            cursor.execute(f"""
            DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

            WITH CTE AS (
                SELECT 
                    CONVERT(INT, ResourceID) AS ResourceID,
                    ISNULL(d.ProjectRoleID, -1) AS ProjectRoleID,
                    ISNULL(b.BlueAntProjectID, -1) AS BlueAntProjectID,
                    ISNULL(c.UserID, -1) AS UserID,
                    a.InsertDate,
                    a.UpdateDate,
                    a.HashkeyBK,
                    CONCAT(a.HashkeyValue, '|', ISNULL(b.BlueAntProjectID, -1), '|', ISNULL(c.UserID, -1), '|', ISNULL(d.ProjectRoleID, -1)) AS HashkeyValue
                FROM {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} a
                LEFT JOIN dwh.DimBlueAntProject b ON a.ProjectBK = b.ProjectBK
                LEFT JOIN dwh.DimUser c ON a.PersonBK = c.BlueAntUserBK
                LEFT JOIN dwh.DimBlueAntProjectRoles d ON a.ProjectRoleBK = d.ProjectRoleBK
            )

            MERGE {SCHEMA_NAME}.{TABLE_NAME} AS TARGET
            USING CTE AS SOURCE
            ON TARGET.HashkeyBK = SOURCE.HashkeyBK 
            WHEN MATCHED AND TARGET.HashkeyValue <> SOURCE.HashkeyValue
            THEN UPDATE SET 
                TARGET.HashkeyValue = SOURCE.HashkeyValue,
                TARGET.ProjectRoleID = SOURCE.ProjectRoleID,
                TARGET.BlueAntProjectID = SOURCE.BlueAntProjectID,
                TARGET.UserID = SOURCE.UserID,
                TARGET.UpdateDate = SOURCE.UpdateDate
            -- When no records are matched, insert the incoming records from the source table to the target table
            WHEN NOT MATCHED BY TARGET THEN 
            INSERT (ResourceID, ProjectRoleID, BlueAntProjectID, UserID, HashkeyBK, HashkeyValue, InsertDate, UpdateDate) 
            VALUES (SOURCE.ResourceID, SOURCE.ProjectRoleID, SOURCE.BlueAntProjectID, SOURCE.UserID, SOURCE.HashkeyBK, SOURCE.HashkeyValue, SOURCE.InsertDate, SOURCE.UpdateDate)
            OUTPUT $action AS [Status] INTO @MergeLog;

            INSERT INTO tmp.[changed] ([Status], Anzahl)
            SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
            GROUP BY [Status];

            """)
            conn.commit()
        except Exception as e:
            logging.error(f"Error while inserting/updating the data into the DWH: {e}")
            conn.rollback()
            raise
        
        logging.info(f"The data was successfully inserted into the DWH: {TABLE_NAME} table.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed.", status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
