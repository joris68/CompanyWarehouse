


CREATE VIEW [pln].[vw_DimMITARBEITERAttr] AS 

  SELECT
[UserID] AS pElement
,[UserInitials] AS pUserInitials
,IIF(  [JiraUserBK] = 'N/A', 'B', 'A' ) + [UserInitials] AS ORDERBY

      ,[UserName] AS pUserName
      ,[UserEmail] AS pUserEmail
      ,[TeamName] AS pTeamName
      ,[State] AS pState
  FROM [pln].[vw_DimUser]
WHERE UserID <> 50

