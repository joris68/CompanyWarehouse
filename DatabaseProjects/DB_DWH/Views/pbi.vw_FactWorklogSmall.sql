
CREATE VIEW [pbi].[vw_FactWorklogSmall] AS 

with cte as (
    SELECT 
      [UserID]
      ,[BlueAntProjectID]     
      ,AVG([ExternalCostPerHour])  AS HourlyRate 
    FROM [pbi].[vw_DimProjectRole]
    GROUP BY [UserID], [BlueAntProjectID]
)

SELECT
  BlueAntTimeMinutes
  , w.BlueAntProjectID
  , JiraTimeSpentMinutes
  , ISNULL(BlueAntIssue, JiraIssue)                     AS IssueID
  , Billable
  , ISNULL(BlueAntUserID,JiraUserID)                    AS UserID
  , ISNULL(BlueAntWorklogDateID,JiraWorklogDateID)      AS DateID
  ,ISNULL(BlueAntWorklogSummary,JiraWorklogSummary)		AS WorklogSummary
  ,BlueAntProjectTaskID
  ,HourlyRate
  ,BlueAntTimeMinutes / 60 * HourlyRate                 AS CostWorkhours
FROM [dwh].[FactWorklog] w
LEFT JOIN cte ON w.BlueAntProjectID = cte.BlueAntProjectID AND w.BlueAntUserID = cte.UserID 


UNION

SELECT 
    BlueAntTimeSpentMinute                              AS BlueAntTimeMinutes
    ,h.BlueAntProjectID
    ,NULL                                               AS JiraTimeSpentMinutes
    ,JiraIssue                                          AS IssueID
    ,Billable                                           AS Billable
    ,h.UserID
    ,BlueAntWorklogDateID                               AS DateID
	,BlueAntWorklogSummary                              AS WorklogSummary
    ,ISNULL([BlueAntProjectTaskID], -1)                 AS BlueAntProjectTaskID
    ,HourlyRate
    ,BlueAntTimeSpentMinute / 60 * HourlyRate                 AS CostWorkhours
FROM [dwh].[FactBlueAntWorklogHistory] h  
LEFT JOIN cte ON h.BlueAntProjectID = cte.BlueAntProjectID AND h.UserID = cte.UserID   
  

