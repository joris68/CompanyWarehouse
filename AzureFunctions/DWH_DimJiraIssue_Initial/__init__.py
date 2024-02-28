#Author: Pascal Schütze 
import logging
import pandas as pd
import hashlib
import json
from common import get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string,get_blob_start_with
import traceback
from datetime import datetime 
import pytz

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:

        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area "blueantprojectservice"
        # -----------------------------------------------------------
    
        CONTAINER = 'initialloads'

        now = get_time_in_string()[0]
    
        blob_content = get_blob_start_with(CONTAINER,'jiraissue').content_as_text()  

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

        # Load the JSON data into a dictionary
        data = json.loads(blob_content)
        
        Jira_Issue_Ids = []
        jira_issue_type_ids = []
        jira_issues = []
        user_assigned_ids = []
        jira_parent_issue_ids = []
        jira_project_ids = []
        sprint_ids = []
        jira_issue_names = []
        datetime_list =[]

        for issue in data: 
            if issue["id"] not in Jira_Issue_Ids:
                Jira_Issue_Ids.append(issue["id"])
                jira_issues.append(issue["key"])
                jira_issue_type_ids.append(issue['fields']['issuetype']['id'])
                jira_project_ids.append(issue['fields']['project']['id'])
                jira_issue_names.append(issue['fields']['summary'])

                if "fields" in issue and "assignee" in issue["fields"] and issue["fields"]["assignee"] is not None:
                    user_assigned_ids.append(issue['fields']['assignee']['accountId'])
                else:
                    user_assigned_ids.append(-1)
                
                if "fields" in issue and "parent" in issue["fields"] and issue["fields"]["parent"] is not None:
                    jira_parent_issue_ids.append(issue['fields']['parent']['id'])
                else:
                    jira_parent_issue_ids.append(-1)

                if 'fields' in issue and 'customfield_10020' in issue['fields'] and isinstance(issue['fields']['customfield_10020'], list):
                    sprint_ids.append(issue['fields']['customfield_10020'][0]['id'])
                else:
                    sprint_ids.append(-1)
                datetime_list.append(now)
                    
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------
        
        hashkeyBK_list = [hashlib.sha256(str(Jira_Issue_Ids[x]).encode()).hexdigest() for x in range(0, len(Jira_Issue_Ids))]

        hashkeyValue_list = [hashlib.sha256((str(jira_issue_type_ids[x]) + str(jira_issues[x]) + str(user_assigned_ids[x]) + str(jira_parent_issue_ids[x]) + str(jira_project_ids[x]) + str(sprint_ids[x]) + str(jira_issue_names[x])).encode()).hexdigest() for x in range(0, len(jira_issue_type_ids))]

        dataset = list(zip(Jira_Issue_Ids, jira_parent_issue_ids, jira_issues, jira_issue_names, user_assigned_ids, jira_issue_type_ids, jira_project_ids, sprint_ids,datetime_list,datetime_list, hashkeyBK_list, hashkeyValue_list))
        
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables.  
        #----------------------------------------------------------------------------------------
    
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimJiraIssue"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimJiraIssue"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        
        InsertQuery = f"""
            INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                (JiraIssueBK,
                JiraParentIssueBK,
                JiraIssue,
                JiraIssueName,
                UserAssignedID,
                JiraIssueTypeID,
                JiraProjectID,
                SprintID,
                InsertDate,
                UpdateDate,
                HashkeyBK,
                HashkeyValue)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)

    #----------------------------------------------------------------------------------------
    # Now, merge data from tmp Table to dwh Table. 
    # Before replace the BKs with the IDs for UserID, JiraProjectID, SprintID and JiraIssueTypeID. 
    #----------------------------------------------------------------------------------------

        cursor.execute(f"""
                       
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));
            WITH CTE AS (
                SELECT
                    tmp.[JiraIssueBK],
                    tmp.[JiraParentIssueBK],
                    tmp.[JiraIssue],
                    tmp.[JiraIssueName],
                    ISNULL(dwh_u.[UserID], -1) as [UserID],
                    ISNULL(dwh_is.[JiraIssueTypeID], -1) as JiraIssueTypeID,
                    ISNULL(dwh_p.[JiraProjectID], -1) as JiraProjectID,
                    ISNULL(dwh_s.JiraSprintID, -1) as JiraSprintID,
                    tmp.[InsertDate],
                    tmp.[UpdateDate],
                    tmp.[HashkeyBK], 
                    CONCAT(tmp.[HashkeyValue], '|', ISNULL(dwh_s.JiraSprintID,-1), '|', ISNULL(dwh_u.UserID,-1), '|', ISNULL(dwh_p.[JiraProjectID],-1), '|', ISNULL(dwh_is.[JiraIssueTypeID],-1)) as [HashkeyValue]
                FROM
                    [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp 
                    LEFT JOIN [{SCHEMA_NAME}].[DimUser] dwh_u ON tmp.[UserAssignedID] = dwh_u.[JiraUserBK]
                    LEFT JOIN [{SCHEMA_NAME}].[DimJiraProject] dwh_p ON tmp.[JiraProjectID] = dwh_p.[JiraProjectBK]
                    LEFT JOIN [{SCHEMA_NAME}].DimJiraSprint dwh_s ON tmp.[SprintID] = dwh_s.SprintBK
                    LEFT JOIN [{SCHEMA_NAME}].DimJiraIssueType dwh_is ON tmp.JiraIssueTypeID = dwh_is.JiraIssueTypeBK
            )

            
            -- MERGE statement
            MERGE  [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
            USING CTE AS SRC
            ON (TARGET.[HashkeyBK] = SRC.[HashkeyBK])
            WHEN MATCHED AND TARGET.[HashkeyValue] <> SRC.[HashkeyValue]
            THEN UPDATE SET TARGET.[HashkeyValue] = SRC.[HashkeyValue],
                TARGET.[JiraIssueBK] = SRC.[JiraIssueBK], 
                TARGET.[JiraParentIssueBK] = SRC.[JiraParentIssueBK],
                TARGET.[JiraIssue] = SRC.[JiraIssue],
                TARGET.[JiraIssueName] = SRC.[JiraIssueName],
                TARGET.[UserID] = SRC.[UserID],
                TARGET.[JiraIssueTypeID] = SRC.[JiraIssueTypeID],
                TARGET.[JiraProjectID] = SRC.[JiraProjectID],
                TARGET.[JiraSprintID] = SRC.[JiraSprintID],
                TARGET.[UpdateDate] = SRC.[UpdateDate]
            WHEN NOT MATCHED BY TARGET 
                THEN 
                    INSERT (
                        [JiraIssueBK], 
                        [JiraParentIssueBK], 
                        [JiraIssue], 
                        [JiraIssueName], 
                        [UserID], 
                        [JiraIssueTypeID], 
                        [JiraProjectID], 
                        [JiraSprintID], 
                        [InsertDate], 
                        [UpdateDate], 
                        [HashkeyBK], 
                        [HashkeyValue]
                    )
                    VALUES (
                        SRC.[JiraIssueBK], 
                        SRC.[JiraParentIssueBK], 
                        SRC.[JiraIssue], 
                        SRC.[JiraIssueName], 
                        SRC.[UserID], 
                        SRC.[JiraIssueTypeID], 
                        SRC.[JiraProjectID], 
                        SRC.[JiraSprintID], 
                        SRC.[InsertDate], 
                        SRC.[UpdateDate], 
                        SRC.[HashkeyBK], 
                        SRC.[HashkeyValue]
                    )       
            OUTPUT  $action as [Status] into @MergeLog;
            INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status],Anzahl)
            SELECT [Status], count(*) as Anzahl FROM @MergeLog
            GROUP BY [Status];

        """)

        # Commit transaction and log success
        conn.commit()

        #Update all DeleteFlags in Dimension
        cursor.execute(f"""
                       
            BEGIN TRANSACTION; 
            BEGIN TRY 
                BEGIN 
                    --erstmal alle deleteflags auf standard setzen und dann wird neu geupdated
                    UPDATE [{SCHEMA_NAME}].[{TABLE_NAME}] SET Deleted = 0,DeletedTimestamp = NULL
                    ;WITH cte_deleteBks AS (
                        SELECT 
                            JiraIssueBK
                        FROM [{SCHEMA_NAME}].[{TABLE_NAME}] 
                    )
                    , cte_tmp AS (
                        SELECT 
                            *
                        FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] 
                    )
                    , cte_deletes AS (
                        SELECT JiraIssueBK FROM cte_deleteBks
                        WHERE JiraIssueBK NOT IN (SELECT JiraIssueBK FROM cte_tmp)
                    )
                    UPDATE [{SCHEMA_NAME}].[{TABLE_NAME}]
                    SET 
                        Deleted = 1,
                        DeletedTimestamp = GETDATE()
                    WHERE 
                        JiraIssueBK IN (SELECT JiraIssueBK FROM cte_deletes)
                        AND Deleted IS NULL;
                       
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
        
        #----------------------------------------------------------------------------------------
        # Now, update the Log Table for DimJiraIssue. 
        #----------------------------------------------------------------------------------------

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor) 
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)