

CREATE VIEW [pbi].[vw_FactCapacity] AS 

SELECT c.[UserID]
      ,c.[DateID]
      ,[WorktimeHours]
      ,t.TargetHours
FROM [tmp].[dwh_FactCapacity] c
LEFT JOIN dwh.DimDate d ON c.DateID = d.DateID
LEFT JOIN pbi.vw_DimTarget t ON c.UserID = t.UserID AND d.YearID = t.YearID
