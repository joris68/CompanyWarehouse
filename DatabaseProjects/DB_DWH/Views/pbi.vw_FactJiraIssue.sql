CREATE VIEW [pbi].[vw_FactJiraIssue] AS

with cte as (
    SELECT 
      [UserID]
      ,[BlueAntProjectID]     
      ,AVG([ExternalCostPerHour])  AS HourlyRate 
    FROM [pbi].[vw_DimProjectRole]
    GROUP BY [UserID], [BlueAntProjectID]
)

SELECT 
      f.[JiraIssueID]
      ,ISNULL([OriginalTimeEstimate], 0) / 60 AS OriginalTimeEstimate
      ,ISNULL([TimeSpent], 0)  / 60 AS TimeSpent
      ,ISNULL([CurrentTimeEstimate], 0)  /60 AS CurrentTimeEstimate
      ,cte.HourlyRate
      ,ISNULL([OriginalTimeEstimate], 0) / 60 /60 * cte.HourlyRate AS OriginalTimeEstimateCost
      ,ISNULL([TimeSpent], 0)  / 60 /60 * cte.HourlyRate AS TimeSpentCost
      ,ISNULL([CurrentTimeEstimate], 0)  /60 /60 * cte.HourlyRate AS CurrentTimeEstimateCost
  FROM [dwh].[FactJiraIssue] f
  LEFT JOIN [pbi].[vw_DimJiraIssue] d ON f.JiraIssueID = d.JiraIssueID
  LEFT JOIN cte ON d.BlueAntProjectID = cte.BlueAntProjectID AND d.UserID = cte.UserID

