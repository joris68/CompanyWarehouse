CREATE VIEW [pbi].[vw_DimJiraIssue] AS

SELECT d.[JiraIssueID]
      ,d2.[JiraIssueID] AS ParentJiraIssueID
      ,d.[JiraIssue]
      ,d.[JiraIssueName]
      ,d.[UserID]
      ,d.[JiraSprintID]
      ,t.JiraIssueType
      ,CASE 
        WHEN d.JiraParentIssueBK = -1 THEN ISNULL(e2.[BlueAntProjectID], -1)
        ELSE ISNULL(e.[BlueAntProjectID], -1) END AS BlueAntProjectID     
  FROM [dwh].[DimJiraIssue] d
  LEFT JOIN [dwh].[DimJiraIssue] d2 ON d.JiraParentIssueBK = d2.JiraIssueBK AND d.JiraParentIssueBK <> -1
  LEFT JOIN [dwh].[RelJiraToBlueAnt] e ON d2.[JiraIssueID] = e.[JiraIssueID]
  LEFT JOIN [dwh].[RelJiraToBlueAnt] e2 ON d.[JiraIssueID] = e2.[JiraIssueID] AND d.JiraParentIssueBK = -1
  LEFT JOIN [dwh].[DimJiraIssueType] t ON d.JiraIssueTypeID = t.JiraIssueTypeID