


CREATE view [pln].[vw_ProjectMITARBEITER]
AS

 SELECT  p.[BlueAntProjectID]
      ,[ProjectNumber]
      ,[ProjectManagerID]
      ,[CustomerID]
      ,[ProjectBK]
      ,[ProjectName]
      ,StartDateID
      ,EndDateID
      ,r.UserID
  FROM [dwh].[DimBlueAntProject] p
  LEFT JOIN [dwh].[FactBlueAntProjectResource] r ON p.BlueAntProjectID = r.BlueAntProjectID
  INNER JOIN pln.vw_DimUser u ON r.UserID = u.UserID   -- filter unknown users
  WHERE ProjectStateID IN (1,2)

 
