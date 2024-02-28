#Author: Pascal Schütze 
import datetime
import logging
import hashlib
import json
from common import get_latest_blob_from_staging, get_relative_blob_path, create_file_name, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string,truncate_tmp_table,insert_into_tmp_table
import traceback


import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimJiraSprint"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimJiraSprint"

        CONTAINER = 'jirasprints'

        now = get_time_in_string()[0]

        #----------------------------------------------------------------------------------------
        # First, retrieve the data from the blob storage 
        #----------------------------------------------------------------------------------------  
        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())
        blob_content = blob.content_as_text()

        # Load the JSON data into a dictionary
        data = json.loads(blob_content)

        #----------------------------------------------------------------------------------------
        # Now, retrive the data from json and pack it into variables that are loaded into DB 
        #----------------------------------------------------------------------------------------

        Jira_Sprint_IDs_list = []
        jira_sprint_names_list = []
        jira_start_dates_list = []
        jira_end_dates_list = []
        hashkeyvalue_list = []
        hashkeybk_list = []
        datetime_list =[]

        for sprint in data["values"]:
            if sprint["id"] not in Jira_Sprint_IDs_list:
                Jira_Sprint_IDs = sprint["id"]
                jira_sprint_names= sprint["name"]
                
                jira_start_date_toDate = datetime.datetime.strptime(sprint["startDate"], '%Y-%m-%dT%H:%M:%S.%fZ')
                year, month, day = jira_start_date_toDate.year, jira_start_date_toDate.month, jira_start_date_toDate.day
                jira_start_date = year * 10000 + month * 100 + day
                jira_start_dates= jira_start_date

                jira_end_date_toDate = datetime.datetime.strptime(sprint["endDate"], '%Y-%m-%dT%H:%M:%S.%fZ')
                year, month, day = jira_end_date_toDate.year, jira_end_date_toDate.month, jira_end_date_toDate.day
                jira_end_date = year * 10000 + month * 100 + day
                jira_end_dates= jira_end_date

                Jira_Sprint_IDs_list.append(Jira_Sprint_IDs)
                jira_sprint_names_list.append(jira_sprint_names)
                jira_start_dates_list.append(jira_start_dates)
                jira_end_dates_list.append(jira_end_dates)

                bk = str(Jira_Sprint_IDs)
                bk_hash = hashlib.sha256(bk.encode()).hexdigest()
                hashkeybk_list.append(bk_hash)

                value = str(jira_sprint_names) + str(jira_start_dates) + str(jira_end_dates) 
                value_hash = hashlib.sha256(value.encode()).hexdigest()
                hashkeyvalue_list.append(value_hash)

                datetime_list.append(now)

        logging.info( "Data successfully loaded into variables.")

        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.  
        #----------------------------------------------------------------------------------------
    
        conn , cursor = init_DWH_Db_connection()

        logging.info("Starting data loading process...")

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        # Truncate the temporary table
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        dataset = list(zip(Jira_Sprint_IDs_list,jira_sprint_names_list,jira_start_dates_list,jira_end_dates_list,datetime_list,datetime_list,hashkeybk_list,hashkeyvalue_list))

        insert_tmp_statement = f"""
                INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                    (SprintBK, SprintName, StartDateID, EndDateID, InsertDate, UpdateDate, HashkeyBK, HashkeyValue)
                VALUES
                    (?, ?, ?, ?, ?,?, ?, ?)
            """

        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        logging.info( "Data inserted into temporary table.")

        #----------------------------------------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table. 
        #----------------------------------------------------------------------------------------

        cursor.execute(f"""
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));
            WITH CTE AS (
                SELECT tmp.[SprintBK], tmp.[SprintName], tmp.[StartDateID], tmp.[EndDateID], tmp.[InsertDate], tmp.[UpdateDate], tmp.[HashkeyBK], tmp.[HashkeyValue]
                FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp 
            )
            
            -- MERGE statement
            MERGE  [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
            USING CTE AS SOURCE
            ON (TARGET.[HashkeyBK] = SOURCE.[HashkeyBK])
            WHEN MATCHED AND TARGET.[HashkeyValue] <> SOURCE.[HashkeyValue]
            THEN UPDATE SET TARGET.[HashkeyValue] = SOURCE.[HashkeyValue],
                TARGET.[SprintBK] = SOURCE.[SprintBK], 
                TARGET.[SprintName] = SOURCE.[SprintName],
                TARGET.[StartDateID] = SOURCE.[StartDateID],
                TARGET.[EndDateID] = SOURCE.[EndDateID],
                TARGET.[UpdateDate] = SOURCE.[UpdateDate]
            WHEN NOT MATCHED BY TARGET 
            THEN INSERT ([SprintBK], [SprintName], [StartDateID], [EndDateID], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
            VALUES (SOURCE.[SprintBK], SOURCE.[SprintName], SOURCE.[StartDateID], SOURCE.[EndDateID], SOURCE.[InsertDate], SOURCE.[UpdateDate], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue])
            OUTPUT  $action as [Status] into @MergeLog;

            INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status],Anzahl)
            SELECT [Status], count(*) as Anzahl FROM @MergeLog
            GROUP BY [Status];

            """)

        # Commit transaction and log success
        conn.commit()

        cursor.commit()

        logging.info("The Merge statement was successfully executed.")

    #----------------------------------------------------------------------------------------
    # Now, update the Log Table for DimJiraSprint. 
    #----------------------------------------------------------------------------------------

        DWH_table_logging(TABLE_NAME, cursor, conn , is_Dimension=True)

        # Close the database connection and log disconnection
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:

       # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
