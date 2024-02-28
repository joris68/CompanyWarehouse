CREATE view [pbi].[vw_DimUser]
AS 

SELECT u.[UserID]
      ,[UserInitials]
      ,[FirstName]
      ,[UserName]
      ,[UserEmail]
      ,ISNULL(TeamName, '<N/A>') AS TeamName
      ,ISNULL(State, '<N/A>') AS State
      ,ISNULL([TeamGroup], '<N/A>') AS TeamGroup
      ,ISNULL([TeamNameSortId], -1) AS TeamNameSortId
      ,ISNULL([TeamGroupSortId], -1) AS TeamGroupSortId
      ,ISNULL([BlueAntUserSortId], UserID) AS BlueAntUserSortId
  FROM [dwh].[DimUser] U
  LEFT JOIN [tmp].[dwh_DimUserExtended] ua ON U.BlueAntUserBK = ua.BlueAntUserBK
  WHERE u.UserName <> 'N/A'

