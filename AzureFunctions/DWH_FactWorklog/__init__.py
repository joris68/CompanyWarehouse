#Autor:MC
import traceback
import azure.functions as func
from common import init_DWH_Db_connection, close_DWH_DB_connection


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        conn, cursor = init_DWH_Db_connection() 

        cursor.execute("truncate table dwh.FactWorklog")
        conn.commit()

        cursor.execute("""
        
        with cte_aggjira as
            (SELECT 
            b.JiraIssueID
            ,a.UserID
            ,concat(b.JiraIssue,'|',a.UserID,'|',a.WorklogEntryDateID,'|',RTRIM(LTRIM(a.WorklogSummary))) as JoinBk
            ,SUM(convert(int,JiraTimeSpentMinutes)) as JiraTimeSpentMinutes
            ,b.JiraIssue
            ,a.WorklogEntryDateID as JiraWorklogDateID
            ,a.WorklogSummary
            FROM dwh.FactJiraWorklog a
            LEFT JOIN dwh.DimJiraIssue b
            on a.JiraIssueID = b.JiraIssueID
            group by 
            b.JiraIssueID
            ,a.UserID
            ,b.JiraIssue
            ,a.WorklogEntryDateID
            ,a.WorklogSummary
            )

            --AggFactWorklog für BA:
            ,cte_ba as(
            select
            IIF(FirstValue = N'N/A',ThirdValue,FirstValue) as JiraIssue
            ,UserID
            ,BlueAntTimeSpentMinute
            ,BlueAntProjectID
            ,WorklogDateID as BlueAntWorklogDateID
            ,Billable
            ,Comment
            ,BlueAntProjectTaskID
            from dwh.FactBlueAntWorklog a
            )
            ,cte_ba2 as(
            Select
            convert(nvarchar(255),JiraIssue) as JiraIssue
            ,UserID
            ,BlueAntTimeSpentMinute
            ,BlueAntProjectID
            ,BlueAntWorklogDateID
            ,Billable
            ,Comment
            ,BlueAntProjectTaskID
            from cte_ba
            )
            ,cte_aggba as(
            select 
            CONCAT(JiraIssue,'|',UserID,'|',BlueAntWorklogDateID,'|',RTRIM(LTRIM(Comment))) as Bk
            ,SUM(BlueAntTimeSpentMinute) as BaTime
            ,JiraIssue
            ,UserID
            ,BlueAntProjectID
            ,BlueAntWorklogDateID
            ,Billable
            ,Comment
            ,BlueAntProjectTaskID
            from cte_ba2
            group by 
            JiraIssue
            ,UserID
            ,BlueAntProjectID
            ,BlueAntWorklogDateID
            ,Billable
            ,Comment
            ,BlueAntProjectTaskID
            )
            , cte_final as(
            select 
            a.Bk as [BlueAntBk]
            ,a.BaTime as [BlueAntTimeMinutes]
            ,a.JiraIssue as [BlueAntIssue]
            ,a.UserID as [BlueAntUserID]
            ,a.BlueAntProjectID
            ,a.BlueAntWorklogDateID
            ,a.Billable
            ,a.BlueAntProjectTaskID
            ,b.JoinBk as JiraBK
            ,b.UserID as JiraUserID
            ,convert(decimal(9,2),b.JiraTimeSpentMinutes) as JiraTimeSpentMinutes
            ,b.JiraIssue as JiraIssue
            ,b.JiraWorklogDateID
            --,ISNULL(Bk,JoinBk) as Factbk
            ,a.Comment as BlueAntWorklogSummary
            ,b.WorklogSummary as JiraWorklogSummary
            from cte_aggba a
            full outer join cte_aggjira b
            on a.Bk = b.JoinBk
            )

        INSERT INTO dwh.FactWorklog
        SELECT * FROM cte_final;
        """
        )
        conn.commit()
        
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)
    
    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)


