#Autor:MC
import logging
import azure.functions as func
import json
import hashlib
import traceback
from common import get_relative_blob_path,  get_latest_blob_from_staging, init_DWH_Db_connection, insert_into_loadlogging, close_DWH_DB_connection, DWH_table_logging,get_time_in_string,get_blob_start_with
from datetime import datetime
import pytz
import os 
import requests


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'initialloads'

        now = get_time_in_string()[0]
    
        blob = get_blob_start_with(CONTAINER,'jirawork').content_as_text()  

        data = json.loads(blob)

        # Initialize lists to store extracted data
        WorklogIds = []
        IssueIds = []
        TimeSpentSeconds = []
        Summaries = []
        StartDates = []
        AuthorAccountIds = []
        Datetimes = []
        HashKeyValues = []
        HashKeyBKs = []
        
        # Extract values from data
        for item in data:
            values = item["worklogs"]
            for item2 in values:
                # Assign values to new variables with updated names
                WorklogId = item2["id"]
                IssueId = item2["issueId"]
                TimeSpentSecond = item2["timeSpentSeconds"]
                StartDate = item2["started"]

                try:
                    #MC_20231121: Abgesprochen mit DBR. Wenn ein Kommentar nach allen Möglichkeiten nicht gelesen werden kann soll Fehlermeldung: DWH-Error Zeiteintrag stehen im Comment
                    # Extract summary with nested checks
                    if "comment" in item2: 
                        if item2["comment"]["content"]:
                            if item2["comment"]["content"][0]["content"]: 
                                if item2["comment"]["content"][0]["content"][0]["text"]:                     
                                    Summary = item2["comment"]["content"][0]["content"][0]["text"]
                                else: 
                                    Summary = "N/A"
                            else: 
                                Summary = "N/A"
                        else: 
                            Summary = "N/A"
                    else: 
                        Summary = "N/A"

                except:
                    Summary = 'DWH-Errorzeiteintrag'
                    
                Summaries.append(Summary)
                
                AuthorAccountId = item2["author"]["accountId"]
                
                # Append values to the new lists with updated variable names
                WorklogIds.append(WorklogId)
                IssueIds.append(IssueId)
                TimeSpentSeconds.append(TimeSpentSecond)
                StartDates.append(StartDate)
                AuthorAccountIds.append(AuthorAccountId)
                Datetimes.append(now)
            
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------
                    
        HashKeyBKs = [hashlib.sha256(str(WorklogIds[x]).encode()).hexdigest() for x in range(0, len(WorklogIds))]
        
        HashKeyValues = [hashlib.sha256((str(IssueIds[x]) + str(TimeSpentSeconds[x]) + str(StartDates[x]) + str(AuthorAccountIds[x])).encode()).hexdigest() for x in range(0, len(IssueIds))]
       
        conn, cursor = init_DWH_Db_connection()

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactJiraWorklog"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactJiraWorklog"
        
        #MC: 28.01 2024 doppelte Connection
        #conn, cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        cursor.execute(f"Truncate Table [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")
        logging.info(f"Truncated tmp table [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")

        cursor.execute(f"Truncate Table [{SCHEMA_NAME}].[{TABLE_NAME}]")
        logging.info(f"Truncated Table [{SCHEMA_NAME}].[{TABLE_NAME}]")

        dataset = list(zip(
            WorklogIds,
            IssueIds,
            TimeSpentSeconds,
            Summaries,
            StartDates,
            AuthorAccountIds,
            Datetimes,
            Datetimes,
            HashKeyBKs,
            HashKeyValues
        ))


        insert_tmp_statement = f""" 
            INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                (JiraWorklogID,
                JiraIssueID,
                JiraTimeSpent,
                WorklogSummary,
                WorklogEntryDate,
                JiraUserBK,
                InsertDate,
                UpdateDate,
                HashkeyBK,
                HashkeyValue)
            VALUES
                (?,?,?,?,?,?,?,?,?,?)
        """
        
        cursor.executemany(insert_tmp_statement, dataset)

        cursor.execute(f"""
                       
            BEGIN TRANSACTION; 
            BEGIN TRY 
            BEGIN           

                -- Synchronize the target table with refreshed data from source table
                WITH cte AS
                (
                    SELECT 
                        CONVERT(INT, a.[JiraWorklogID]) AS [JiraWorklogID],
                        ISNULL(c.JiraIssueID,-1) as JiraIssueID,
                        a.JiraIssueID AS JiraIssueBK,
                        CONVERT(DECIMAL(9, 2), a.[JiraTimeSpent] / 60) AS [JiraTimeSpentMinutes],
                        a.WorklogSummary,
                        CAST(CONVERT(VARCHAR(10), CONVERT(DATE, LEFT(a.[WorklogEntryDate], 10)), 112) AS INT) AS WorklogEntryDateID,
                        ISNULL(b.UserID,-1) as UserID,
                        a.InsertDate,
                        a.UpdateDate,
                        a.HashkeyBK,
                        LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                            CONCAT(
                                    CONVERT(VARCHAR, a.JiraIssueID),'|',
                                    CONVERT(VARCHAR, a.JiraTimeSpent),'|',
                                    CONVERT(VARCHAR, a.WorklogSummary),'|',
                                    CONVERT(VARCHAR, CAST(CONVERT(VARCHAR(10), CONVERT(DATE, LEFT(a.WorklogEntryDate, 10)), 112) AS INT)),'|',
                                    CONVERT(VARCHAR, ISNULL(b.UserID,-1)),'|',
                                    CONVERT(VARCHAR, ISNULL(c.JiraIssueID,-1))
                            )
                        ), 2)) AS HashkeyValue  -- New hashkey generation because of the LookUp 
                    FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] a
                    LEFT JOIN [{SCHEMA_NAME}].dimuser b ON a.[JiraUserBK] = b.JiraUserBK
                    LEFT JOIN [{SCHEMA_NAME}].DimJiraIssue c ON a.[JiraIssueID] = c.JiraIssueBK
                )
                        
                INSERT INTO [{SCHEMA_NAME}].[{TABLE_NAME}] (
                    [JiraWorklogID], 
                    [JiraIssueID],
                    JiraIssueBK, 
                    [JiraTimeSpentMinutes],
                    [WorklogSummary],
                    WorklogEntryDateID,
                    [UserID],
                    InsertDate,
                    UpdateDate,
                    HashkeyBK,
                    HashkeyValue
                ) 
                SELECT 
                    SRC.[JiraWorklogID], 
                    SRC.[JiraIssueID],
                    SRC.JiraIssueBK, 
                    SRC.[JiraTimeSpentMinutes],
                    SRC.[WorklogSummary],
                    SRC.WorklogEntryDateID,
                    SRC.[UserID],
                    SRC.InsertDate,
                    SRC.UpdateDate,
                    SRC.HashkeyBK,
                    SRC.HashkeyValue 
                FROM cte SRC;

            END
                                    
            IF @@TRANCOUNT > 0 
                COMMIT TRANSACTION; 
            END TRY 
            BEGIN CATCH 
                IF @@TRANCOUNT > 0 
                    ROLLBACK TRANSACTION; 
                THROW; 
            END CATCH; 

        """)

        conn.commit()

        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
