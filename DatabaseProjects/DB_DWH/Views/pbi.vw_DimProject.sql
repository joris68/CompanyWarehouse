
CREATE VIEW [pbi].[vw_DimProject] as 

SELECT  
      p.[BlueAntProjectID]
      ,[CustomerID]
      ,[ProjectName]
      ,u.UserName AS ProjectManager
      ,TeamName AS ProjectManagerTeamName
	  ,StartDateID
	  ,EndDateID
    ,[ProjectStateName]
    ,b.BudgetValue AS ProjectBudget
  FROM [dwh].[DimBlueAntProject] p
  LEFT JOIN [dwh].[DimBlueAntProjectState] s ON p.ProjectStateID = s.ProjectStateID
  LEFT JOIN [dwh].[FactBlueAntBudget] b ON p.BlueAntProjectID = b.BlueAntProjectID
  LEFT JOIN [pbi].[vw_DimUser] u ON p.ProjectManagerID = u.UserID

UNION 

SELECT  
    -2 AS  [BlueAntProjectID]
    ,-2 AS  [CustomerID]
    ,'Deals' AS [ProjectName]
    ,'<N/A>' AS ProjectManager
    ,'<N/A>' AS ProjectManagerTeamName
	,-1 AS StartDateID
	,-1 AS EndDateID
    ,'Projektvorbereitung' AS [ProjectStateName]
    ,NULL AS ProjectBudget