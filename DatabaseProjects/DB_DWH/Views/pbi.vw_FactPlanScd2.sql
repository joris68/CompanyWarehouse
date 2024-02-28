
CREATE VIEW [pbi].[vw_FactPlanScd2] as 
with cte_projectpalning as (

    SELECT  [BlueAntProjectID]
      ,CASE EmployeeID
            WHEN 0 THEN -2
            ELSE EmployeeID
       END                 AS UserID 
      ,[DateID]
      ,[PlanningValue]
  FROM [dwh].[FactProjectPlanning]),
  
cte as (
SELECT  
    CASE
        WHEN [BlueAntProjectID] = -1 AND [DealID] = -1 THEN 361
        WHEN [BlueAntProjectID] = -1 AND [DealID] <> -1 THEN -2
        ELSE BlueAntProjectID 
        END                                     AS BlueAntProjectID
      ,[DealID]
      ,p.[UserID] 
      ,[Billable]
      ,[PlanningType]
      ,c.DateID
      ,EndDateID
      ,PlanningValue
	  ,p.RowStartDate
	  ,p.RowEndDate
	  ,P.RowIsCurrent
  FROM [dwh].[FactPlanningSCD2] p
  LEFT JOIN [tmp].[dwh_FactCapacity] c ON p.UserID = c.UserID AND c.DateID BETWEEN p.StartDateID AND p.EndDateID
  WHERE p.StartDateID >= 20230901

UNION

  SELECT  
      [BlueAntProjectID]
      , -1                 AS DealID
      ,p.UserID  
      ,1                   AS Billable    
      ,1                   AS PlanningType
      ,c.DateID            
      ,d.LastDayOfMonth    AS EndDateID
      ,[PlanningValue]
	  ,CONVERT(INT,19000101) AS RowStartDate
	  ,CONVERT(INT,20230831) AS RowEndDate
	  ,CONVERT(INT,0)		 AS RowIsCurrent
  FROM cte_projectpalning  p
  INNER JOIN dwh.DimDate d ON p.DateID = d.DateID
  LEFT JOIN [tmp].[dwh_FactCapacity] c ON p.UserID = c.UserID AND c.DateID BETWEEN p.DateID AND d.LastDayOfMonth
  WHERE p.DateID < 20230901

  ),

cte_group as (
select   
      [BlueAntProjectID]
      ,[DealID]
      ,[UserID] 
      ,[Billable]
      ,[PlanningType]
      ,EndDateID
      ,COUNT(*) AS WorkDays
FROM cte 
GROUP BY     
      [BlueAntProjectID]
      ,[DealID]
      ,[UserID] 
      ,[Billable]
      ,[PlanningType]
      ,EndDateID )

SELECT
      c.[BlueAntProjectID] 
      ,c.DealID
      ,c.[UserID] 
      ,c.[Billable]
      ,c.[PlanningType]
      ,c.DateID
      ,c.PlanningValue
      ,cg.WorkDays
      ,c.PlanningValue / cg.WorkDays AS PlanningValuePerDay
	  ,c.RowStartDate
	  ,c.RowEndDate
	  ,c.RowIsCurrent
FROM cte c
LEFT JOIN cte_group cg ON c.BlueAntProjectID = cg.BlueAntProjectID AND c.UserID = cg.UserID AND c.Billable = cg.Billable AND c.PlanningType = cg.PlanningType AND c.EndDateID = cg.EndDateID
