CREATE VIEW [pbi].[vw_FactTeamExtendedCapacity] AS 

with cte as (
SELECT [UserID]
      ,[Capacity]
      ,[StartDateID]
      ,[EndDateID]
      ,d.WeekdayID
      ,d.DateID
FROM [dwh].[FactTeamExtendedCapacity] c
LEFT JOIN dwh.DimDate d ON d.DateID BETWEEN c.StartDateID AND c.EndDateID AND WeekdayID < 6),

cte_group as (
select   
      [UserID] 
      ,EndDateID
      ,COUNT(*) AS WorkDays
FROM cte 
GROUP BY     
      [UserID] 
      ,EndDateID )

SELECT
      c.[UserID] 

      ,c.DateID AS [StartDateID]
      ,c.DateID AS EndDateID
      ,WeekdayID AS Weekyday

      ,c.Capacity/ cg.WorkDays  AS [WorktimeHours]
FROM cte c
LEFT JOIN cte_group cg ON c.UserID = cg.UserID AND c.EndDateID = cg.EndDateID    