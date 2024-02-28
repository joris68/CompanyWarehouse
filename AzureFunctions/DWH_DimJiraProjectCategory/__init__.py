#Autor:MC
import logging
import azure.functions as func
import json
import hashlib
from common import init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging, get_relative_blob_path, create_file_name, get_latest_blob_from_staging
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:
        
    try:

       # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimJiraProjectCategory"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimJiraProjectCategory"

        CONTAINER = 'jiraproject'

        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())
        blob_content = blob.content_as_text()  

        data = json.loads(blob_content)

        values = data['values']

        # values  to extract
        projectcategoryIds = []
        projectCategoryNames = []
        projectCategoryDescriptions = []

        for item in values:
            if 'projectCategory' not in item:
                continue
            categoryobject = item['projectCategory']
            # filling the lists with distinct values
            if categoryobject['id'] not in projectcategoryIds:
                projectcategoryIds.append(categoryobject['id'])
                projectCategoryNames.append(categoryobject['name'])
                projectCategoryDescriptions.append(categoryobject['description'])


    
        conn, cursor = init_DWH_Db_connection()
        
        cursor.execute(f"Truncate Table [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")
        logging.info("Truncated Temp Table")

     # now inserting new values into the temp table

        logging.info("Started Logging")

        insert_into_loadlogging(SCHEMA_NAME,TABLE_NAME, cursor, conn)

    #now inserting into the temp table

        for i in range(len(projectcategoryIds)):
            hashkeyValue = str(projectCategoryNames[i]) + str(projectCategoryDescriptions[i])
            bk_value = str(projectcategoryIds[i])
            hash_objectbk = hashlib.sha256(bk_value.encode())
            hash_objectValue = hashlib.sha256(hashkeyValue.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hex_digVal = hash_objectValue.hexdigest()

            cursor.execute(f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (id, name , description,InsertDate, UpdateDate, HashkeyBK, HashkeyValue) VALUES (?,?,?,getDate(), getDate(), ?,?)", projectcategoryIds[i], projectCategoryNames[i], projectCategoryDescriptions[i], hex_digbk, hex_digVal)
        conn.commit()

        logging.info("temp table was successfully inserted")

    # now the merge statement

        cursor.execute(f"""
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));
        Merge [{SCHEMA_NAME}].[{TABLE_NAME}] as tgt
        using [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] as src
        on tgt.HashkeyBK = src.HashkeyBK
        when matched AND tgt.HashkeyValue <> src.HashkeyValue then
        update set
            tgt.ProjectCategoryBK = src.id ,
            tgt.ProjectCategoryName = src.name,
            tgt.ProjectCategoryDescription = src.description,
            tgt.InsertDate = src.InsertDate,
            tgt.UpdateDate = src.UpdateDate,
            tgt.HashkeyBK = src.HashkeyBK,
            tgt.HashkeyValue = src.HashkeyValue
        when not matched by Target then
        insert([ProjectCategoryBK] , [ProjectCategoryName], [ProjectCategoryDescription],[InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
        VALUES (src.id, src.name, src.description, src.InsertDate, src.UpdateDate ,src.HashkeyBK, src.HashkeyValue)
        OUTPUT $action as [Status] into @MergeLog;
        INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status],Anzahl)
        SELECT [Status], count(*) as Anzahl FROM @MergeLog
        GROUP BY [Status];;""")
    
        conn.commit()

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=True)
        # Close the database connection
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)



