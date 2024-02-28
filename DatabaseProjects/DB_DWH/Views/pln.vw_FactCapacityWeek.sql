CREATE VIEW [pln].[vw_FactCapacityWeek] AS 

SELECT 
    [UserID]
    ,[WeekID]
    ,SUM([WorktimeHours])/8 AS WorkDaysPerWeek
FROM [pln].[vw_FactCapacity]
GROUP BY [UserID], [WeekID]