#Autor:MC
import logging
import os
import azure.functions as func
import json
import hashlib
from common import get_latest_blob_from_staging, get_relative_blob_path, create_file_name, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string,truncate_tmp_table,insert_into_tmp_table
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try: 

        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimJiraUser"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimJiraUser"

        CONTAINER = 'jirauser'

        now = get_time_in_string()[0]

        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())
        blob_content = blob.content_as_text()

        data = json.loads(blob_content)

        AccountIds_list = []
        emailAdress_list = []
        displayName_list = []
        hashkeyvalue_list = []
        hashkeybk_list = []
        datetime_list =[]

        for items in data:
            if items["accountId"] not in AccountIds_list:
                Account_Ids=items["accountId"]
                displayName=items["displayName"]
                if 'emailAddress' in items:
                    emailAdress=items["emailAddress"]
                else:
                    emailAdress="N/A"

                datetime_list.append(now)

                AccountIds_list.append(Account_Ids)
                displayName_list.append(displayName)
                emailAdress_list.append(emailAdress)

                bk=str(Account_Ids)
                hash_objectbk = hashlib.sha256(bk.encode())
                hex_digbk = hash_objectbk.hexdigest()
                hashkeybk_list.append(hex_digbk)

                value= emailAdress + str(displayName)
                hash_objectvalue = hashlib.sha256(value.encode())
                hex_digvalue = hash_objectvalue.hexdigest()
                hashkeyvalue_list.append(hex_digvalue)

        conn, cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        # Truncate the temporary table
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

    #-------------------------------------------------------------------------- JiraUser 

        dataset = list(zip(AccountIds_list,emailAdress_list,displayName_list,hashkeybk_list,hashkeyvalue_list,datetime_list,datetime_list))

        insert_tmp_statement = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (JiraUserBK, UserEmail, UserName,HashkeyBK,HashkeyValue,InsertDate,UpdateDate) VALUES (?, ?, ?,?,?,?,?)"
    
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        cursor.execute("""
        --Synchronize the target table with refreshed data from source table
        DECLARE @MergeLog TABLE([Status] VARCHAR(20));

            MERGE  dwh.DimJiraUser AS TARGET
            USING tmp.dwh_DimJiraUser AS SOURCE
            ON (TARGET.[HashkeyBK] = SOURCE.[HashkeyBK])
            WHEN MATCHED AND TARGET.[HashkeyValue] <> SOURCE.[HashkeyValue]
            THEN UPDATE SET TARGET.[HashkeyValue] = SOURCE.[HashkeyValue],
                TARGET.[JiraUserBK] = SOURCE.[JiraUserBK], 
                TARGET.[UserEmail] = SOURCE.[UserEmail],
                TARGET.[UserName] = SOURCE.[UserName],
                TARGET.[UpdateDate] = SOURCE.[UpdateDate]
            WHEN NOT MATCHED BY TARGET 
            THEN INSERT ([JiraUserBK], [UserEmail], [UserName],[InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
            VALUES (SOURCE.[JiraUserBK], SOURCE.[UserEmail], SOURCE.[UserName], SOURCE.[InsertDate], SOURCE.[UpdateDate], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue])
            OUTPUT  $action as [Status] into @MergeLog;

        INSERT INTO [tmp].[changed] ([Status],Anzahl)
        SELECT [Status], count(*) as Anzahl FROM @MergeLog
        GROUP BY [Status];
        """)
        conn.commit()

        cursor.commit()

        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=True)

        # Close the database connection
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)

