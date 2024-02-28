import logging
import json
import hashlib
import azure.functions as func
from common import get_relative_blob_path, get_latest_blob_from_staging, init_DWH_Db_connection, insert_into_loadlogging, close_DWH_DB_connection, DWH_table_logging,get_time_in_string,truncate_tmp_table,insert_into_tmp_table,get_blob_start_with
import traceback
from datetime import datetime
import pytz


def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:

        CONTAINER = 'initialloads'

        now = get_time_in_string()[0]
    
        blob = get_blob_start_with(CONTAINER,'jiraissue').content_as_text()  

        data = json.loads(blob)

        id_list = []
        timespent_list = []
        timeestimate_list = []
        timeoriginalestimate_list = []
        datetime_list = []
        hashkeyvalue_list= []
        hashkeybk_list = []

        for issue in data:
            id= issue['id']
            json_object = issue['fields']
            timespent= json_object['timespent']
            timeestimate = json_object['timeestimate']
            timeoriginalestimate= json_object['timeoriginalestimate']
            if id not in id_list:
                id_list.append(id)
                timespent_list.append(timespent)
                timeestimate_list.append(timeestimate)
                timeoriginalestimate_list.append(timeoriginalestimate)

                datetime_list.append(now)

                #hashkeybk und haskeyvalue
                bk=str(id)
                hash_objectbk = hashlib.sha256(bk.encode())
                hex_digbk = hash_objectbk.hexdigest()
                hashkeybk_list.append(hex_digbk)

                value= str(timespent) + str(timeestimate) + str(timeoriginalestimate)
                hash_objectvalue = hashlib.sha256(value.encode())
                hex_digvalue = hash_objectvalue.hexdigest()
                hashkeyvalue_list.append(hex_digvalue)

        conn, cursor = init_DWH_Db_connection()

        dataset = list(zip(id_list,timeoriginalestimate_list,timespent_list,timeestimate_list,datetime_list,datetime_list,hashkeybk_list,hashkeyvalue_list))

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactJiraIssue"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactJiraIssue"
        
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        logging.info(f"Truncated Temp Table: [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")

        # now inserting new values into the temp table

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        #-------------------------

        insert_tmp_statement = f"""
            INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                ([JiraIssueBK],
                [Timeoriginalestimate],
                [Timespent],
                [Timeestimate],
                InsertDate,
                UpdateDate,
                HashkeyBK,
                HashkeyValue)
            VALUES
                (?,?,?,?,?,?,?,?)
        """

        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn) 

        logging.info("Tmp table was successfully inserted.")
        
        cursor.commit()

        conn.commit()

        cursor.execute(f"TRUNCATE TABLE [{SCHEMA_NAME}].[{TABLE_NAME}]")

        logging.info(f"Truncated Fact Table: [{SCHEMA_NAME}].[{TABLE_NAME}]")

        conn.commit()
        
        cursor.commit()

        cursor.execute(f"""
                       
            WITH cte AS (
                SELECT
                    ISNULL(d.JiraIssueID, -1) AS JiraIssueID,
                    j.JiraIssueBK,
                    Timeoriginalestimate,
                    Timespent,
                    Timeestimate,
                    j.HashkeyBK,
                    CONCAT(j.HashkeyValue, '|', ISNULL(d.JiraIssueID,-1)) AS HashkeyValue,
                    j.InsertDate,
                    j.UpdateDate
                FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS j
                LEFT JOIN [{SCHEMA_NAME}].[DimJiraIssue] AS d ON d.JiraIssueBK = j.JiraIssueBK
                WHERE d.JiraSprintID != -1
            )

            INSERT INTO [{SCHEMA_NAME}].[{TABLE_NAME}] (
                [JiraIssueID],
                JiraIssueBK,
                [OriginalTimeEstimate],
                [TimeSpent],
                [CurrentTimeEstimate],
                InsertDate,
                UpdateDate,
                [HashkeyBK],
                [HashkeyValue]
            )
            SELECT
                src.JiraIssueID,
                src.JiraIssueBK,
                src.Timeoriginalestimate,
                src.Timespent,
                src.Timeestimate,
                src.InsertDate,
                src.UpdateDate,
                src.HashkeyBK,
                src.HashkeyValue
            FROM cte src;

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













