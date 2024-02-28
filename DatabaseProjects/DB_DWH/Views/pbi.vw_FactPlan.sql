CREATE VIEW [pbi].[vw_FactPlan] as 


with cte_projectpalning as (

    SELECT  
        [BlueAntProjectID]
      ,CASE EmployeeID
            WHEN 0 THEN -2
            ELSE EmployeeID
       END                 AS UserID 
      ,[DateID]
      ,[PlanningValue]  AS PlannedWorktime
  FROM [dwh].[FactProjectPlanning]),
  
cte as (
SELECT  
    CASE
        WHEN [BlueAntProjectID] = -1 AND [DealID] <> -1 THEN -2
        ELSE BlueAntProjectID 
        END                                     AS BlueAntProjectID
      ,[DealID]
      ,p.[UserID] 
      ,[Billable]
      ,[PlanningType]
      ,ISNULL(c.DateID, StartDateID) AS DateID
      ,EndDateID
      ,PlanningValue  AS PlannedWorktime
  FROM [dwh].[FactPlanning] p
  LEFT JOIN [tmp].[dwh_FactCapacity] c ON p.UserID = c.UserID AND c.DateID BETWEEN p.StartDateID AND p.EndDateID
  WHERE p.StartDateID >= 20230901

UNION

  SELECT  
      [BlueAntProjectID]
      , -1                 AS DealID
      ,p.UserID  
      ,1                   AS Billable    
      ,1                   AS PlanningType
      ,ISNULL(c.DateID, p.DateID) AS DateID            
      ,d.LastDayOfMonth    AS EndDateID
      ,PlannedWorktime
  FROM cte_projectpalning  p
  INNER JOIN dwh.DimDate d ON p.DateID = d.DateID
  LEFT JOIN [tmp].[dwh_FactCapacity] c ON p.UserID = c.UserID AND c.DateID BETWEEN p.DateID AND d.LastDayOfMonth
  WHERE p.DateID < 20230901),

cte_role as (
    SELECT 
      [UserID]
      ,[BlueAntProjectID]     
      ,AVG([ExternalCostPerHour])  AS HourlyRate 
    FROM [pbi].[vw_DimProjectRole]
    GROUP BY [UserID], [BlueAntProjectID]
), 

cte_group as (
select   
      [BlueAntProjectID]
      ,[DealID]
      ,[UserID] 
      ,[Billable]
      ,[PlanningType]
      ,PlannedWorktime
      ,EndDateID
      ,COUNT(*) AS WorkDays
FROM cte 
GROUP BY     
      [BlueAntProjectID]
      ,[DealID]
      ,[UserID] 
      ,[Billable]
      ,[PlanningType]
      ,PlannedWorktime
      ,EndDateID )

SELECT
      c.[BlueAntProjectID] 
      ,c.DealID
      ,c.[UserID] 
      ,c.[Billable]
      ,c.[PlanningType]
      ,c.DateID
      ,c.PlannedWorktime / cg.WorkDays AS PlannedWorkPerDay
     ,cte_role.HourlyRate
     ,(c.PlannedWorktime / cg.WorkDays ) * 8 * HourlyRate AS PlannedCostWorkDays
FROM cte c
LEFT JOIN cte_group cg ON c.BlueAntProjectID = cg.BlueAntProjectID AND c.UserID = cg.UserID AND c.Billable = cg.Billable AND c.PlanningType = cg.PlanningType AND c.EndDateID = cg.EndDateID AND c.DealID = cg.DealID AND c.PlannedWorktime = cg.PlannedWorktime
LEFT JOIN cte_role ON c.BlueAntProjectID = cte_role.BlueAntProjectID AND cte_role.UserID  = c.UserID
