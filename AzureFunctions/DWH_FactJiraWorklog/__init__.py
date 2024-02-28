#Autor:MC Complete Rewritten by PSC
import logging
import azure.functions as func
import json
import hashlib
import traceback
from common import get_relative_blob_path, lookup_tmp_table, insert_into_tmp_table, get_latest_blob_from_staging, init_DWH_Db_connection, truncate_tmp_table, insert_into_loadlogging, close_DWH_DB_connection, DWH_table_logging,get_last_two_SprintID,get_time_in_string


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
        
        # -----------------------------------------------------------
        # Load the latest json-file from the staging area "jiraworklog"
        # -----------------------------------------------------------
       
        now = get_time_in_string()[0]

        CONTAINER = 'jiraworklog'

        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())

        data = json.loads(blob.content_as_text())
        
        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

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

                #MC_20231121: Abgesprochen mit DBR. Wenn ein Kommentar nach allen Möglichkeiten nicht gelesen werden kann soll Fehlermeldung: DWH-Error Zeiteintrag stehen im Comment
                #PSC_20240124: Anpassung des Nested Checks, um in jeder tiefe nach dem Comment zu suchen 
                try:
                    # Recursive function to find the comment text in the json structure
                    # The comment text is not always on the same level in the json structure
                    # Therefore, we need to find it dynamically
                    def find_comment_text(content):
                        for item in content:
                            if item.get("type") == "text":
                                return item.get("text")
                            elif item.get("content"):
                                text = find_comment_text(item.get("content"))
                                if text:
                                    return text
                        return None
                    
                    # Get the comment block of the item in the json structure
                    comment_content = item2.get("comment", {}).get("content", [])
                    comment_text = find_comment_text(comment_content)
                    # If the comment text is not found, set it to "N/A"
                    Summary = comment_text if comment_text else "N/A"
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
                    
        dataset = list(zip(WorklogIds, IssueIds, TimeSpentSeconds, Summaries, StartDates, AuthorAccountIds, Datetimes, Datetimes, HashKeyBKs, HashKeyValues))       
        
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.  
        #----------------------------------------------------------------------------------------

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactJiraWorklog"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactJiraWorklog"
        
        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        InsertQuery = f"""
            INSERT INTO {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME}
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

        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)

        #----------------------------------------------------------------------------------------
        # Now, merge the tmp table into the DWH table
        #----------------------------------------------------------------------------------------

        jiralast2sprintids=get_last_two_SprintID()

        cursor.execute(f"""
              
            BEGIN TRANSACTION; 
            BEGIN TRY 
                BEGIN
                    -- Delete der letzten 2 Sprints
                    DECLARE @ActiveSprintID INT = (SELECT JiraSprintID FROM {SCHEMA_NAME}.DimJiraSprint WHERE SprintBK = {jiralast2sprintids[2]});
                    DECLARE @LastSprintID INT = (SELECT JiraSprintID FROM {SCHEMA_NAME}.DimJiraSprint WHERE SprintBK = {jiralast2sprintids[1]});
                    DECLARE @LastLastSprintID INT = (SELECT JiraSprintID FROM {SCHEMA_NAME}.DimJiraSprint WHERE SprintBK = {jiralast2sprintids[0]});

                    WITH cte_deleteBks AS (
                        SELECT 
                            a.JiraWorklogID,
                            a.JiraIssueID,
                            b.JiraSprintID
                        FROM {SCHEMA_NAME}.{TABLE_NAME} a
                        LEFT JOIN {SCHEMA_NAME}.dimjiraissue b ON a.JiraIssueID = b.JiraIssueID
                        WHERE b.JiraSprintID IN (@ActiveSprintID, @LastSprintID, @LastLastSprintID)
                    )
                    DELETE FROM {SCHEMA_NAME}.{TABLE_NAME}
                    WHERE JiraWorklogID IN (SELECT JiraWorklogID FROM cte_deleteBks);

                    DECLARE @MergeLog TABLE([Status] VARCHAR(20));
                    
                    WITH CTE AS (
                        SELECT
                            CONVERT(INT, a.JiraWorklogID) AS JiraWorklogID,
                            ISNULL(c.JiraIssueID,-1) as JiraIssueID,
                            a.JiraIssueID AS JiraIssueBK,
                            CONVERT(DECIMAL(9, 2), a.JiraTimeSpent / 60) AS JiraTimeSpentMinutes,
                            a.WorklogSummary,
                            CAST(CONVERT(VARCHAR(10), CONVERT(DATE, LEFT(a.WorklogEntryDate, 10)), 112) AS INT) AS WorklogEntryDateID,
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

                        FROM {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME}  a
                        LEFT JOIN {SCHEMA_NAME}.dimuser b         ON a.JiraUserBK = b.JiraUserBK
                        LEFT JOIN {SCHEMA_NAME}.DimJiraIssue c    ON a.JiraIssueID = c.JiraIssueBK
                    )

                    -- MERGE statement
                    MERGE {SCHEMA_NAME}.{TABLE_NAME} AS TARGET
                    USING CTE AS SRC
                    ON (TARGET.HashkeyBK = SRC.HashkeyBK)
                    WHEN MATCHED AND TARGET.HashkeyValue <> SRC.HashkeyValue
                    THEN UPDATE SET TARGET.HashkeyValue = SRC.HashkeyValue,
                                    TARGET.JiraIssueID = SRC.JiraIssueID,
                                    TARGET.JiraIssueBK = SRC.JiraIssueBK,
                                    TARGET.JiraTimeSpentMinutes = SRC.JiraTimeSpentMinutes,
                                    TARGET.WorklogSummary = SRC.WorklogSummary,
                                    TARGET.WorklogEntryDateID = SRC.WorklogEntryDateID,
                                    TARGET.UserID = SRC.UserID,
                                    TARGET.UpdateDate = SRC.UpdateDate
                    
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (
                            JiraWorklogID, JiraIssueID, JiraIssueBK, JiraTimeSpentMinutes, WorklogSummary,
                            WorklogEntryDateID, UserID, InsertDate, UpdateDate, HashkeyBK, HashkeyValue
                        )
                        VALUES (
                            SRC.JiraWorklogID, SRC.JiraIssueID, SRC.JiraIssueBK, SRC.JiraTimeSpentMinutes,
                            SRC.WorklogSummary, SRC.WorklogEntryDateID, SRC.UserID, SRC.InsertDate,
                            SRC.UpdateDate, SRC.HashkeyBK, SRC.HashkeyValue
                        );

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
        return func.HttpResponse(str(e) +"\n--\n" + error_traceback, status_code=400)