CREATE view [pln].[vw_FactCapacity] AS 

SELECT   
    CAST(d.DateBK AS VARCHAR(20)) AS DateID
    ,c.[UserID]
    ,CAST(d.CalendarYearID AS VARCHAR(20)) + '-W' + d.CalendarWeekText  AS WeekID
    ,c.[WorktimeHours]    
  FROM [tmp].[dwh_FactCapacity] c
  INNER JOIN dwh.DimDate d ON d.DateID = c.DateID
  WHERE d.YearID IN (2023, 2024)

/*with cte as 
(
SELECT 
    d.[DateID]
    ,d.DateBK
    ,c.[UserID]
    ,CAST(d.CalendarYearID AS VARCHAR(20)) + '-W' + d.CalendarWeekText  AS WeekID
    ,c.[WorktimeHours]
FROM dwh.DimDate d
INNER JOIN [pbi].[vw_FactTeamCapacity] c ON d.DateID BETWEEN c.[StartDateID] AND c.[EndDateID] AND d.WeekdayID = c.[Weekyday]
WHERE d.YearID IN (2023, 2024)

),

cte_holiday as(

SELECT 
    cte.DateID
    ,cte.DateBK
    ,cte.UserID
    ,cte.WeekID
    ,cte.WorktimeHours
FROM cte
LEFT JOIN [pbi].[vw_FactNoWorking] noWork ON cte.UserID = noWork.UserID AND cte.DateID = noWork.HolidayDateID
WHERE  noWork.HolidayDateID IS NULL AND noWork.UserID IS NULL),

cte_absence as (

SELECT  
    d.DateID
    ,a.UserID
FROM dwh.DimDate d
INNER JOIN [dwh].[FactBlueAntAbsence] a ON d.DateID BETWEEN a.DateFromID AND a.DateToID
WHERE a.State = 'released')

SELECT 
    CAST(cte_holiday.DateBK AS VARCHAR(20)) AS DateID
    ,cte_holiday.UserID
    ,cte_holiday.WeekID
    ,cte_holiday.WorktimeHours
FROM cte_holiday
LEFT JOIN cte_absence ON cte_absence.UserID = cte_holiday.UserID AND cte_absence.DateID = cte_holiday.DateID
WHERE  cte_absence.DateID IS NULL AND cte_absence.UserID IS NULL

UNION

SELECT 
    CAST(d.DateBK AS VARCHAR(20)) AS DateID
    ,c.UserID
    ,CAST(d.CalendarYearID AS VARCHAR(20)) + '-W' + d.CalendarWeekText  AS WeekID
    ,c.WorktimeHours
FROM dwh.DimDate d
INNER JOIN [pbi].[vw_FactTeamExtendedCapacity] c ON d.DateID BETWEEN c.[StartDateID] AND c.[EndDateID] AND d.WeekdayID = c.[Weekyday]
WHERE d.YearID IN (2023, 2024)


*/  

