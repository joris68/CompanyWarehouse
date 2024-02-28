CREATE PROCEDURE [dbo].[sp_FillFactCapacity] AS 

TRUNCATE TABLE [tmp].[dwh_FactCapacity];
with cte_capa_basis as (

SELECT 
    [DateID]
    ,YearID
    ,[UserID]   
    ,WorktimeHours
FROM [pbi].[vw_DimDate] d
INNER JOIN [pbi].[vw_FactTeamCapacity] c ON d.DateID BETWEEN c.[StartDateID] AND c.[EndDateID] AND d.WeekdayID = c.[Weekyday]


UNION

SELECT 
    [DateID]
    ,YearID
    ,[UserID]
    ,WorktimeHours
FROM [pbi].[vw_DimDate] d
INNER JOIN [pbi].[vw_FactTeamExtendedCapacity] c ON d.DateID BETWEEN c.[StartDateID] AND c.[EndDateID] AND d.WeekdayID = c.[Weekyday]
),

cte_holiday as (


SELECT 
      c.UserID
      ,YearID
      ,c.DateID
      ,WorktimeHours
FROM cte_capa_basis c
LEFT JOIN [pbi].[vw_FactNoWorking] h ON c.UserID = h.UserID AND c.DateID = h.HolidayDateID
WHERE  h.HolidayDateID IS NULL AND h.UserID IS NULL

),

cte_absence as (

SELECT  
    d.[DateID]
    ,a.[UserID]
FROM [pbi].[vw_DimDate] d
INNER JOIN [dwh].[FactBlueAntAbsence] a ON d.DateID BETWEEN a.DateFromID AND a.DateToID 
WHERE a.[State] = 'released'
    AND a.IsDeleted = 0
)

INSERT INTO [tmp].[dwh_FactCapacity] (UserID, DateID, WorktimeHours)

SELECT 
       c.UserID
      ,c.DateID
      ,c.WorktimeHours
           
FROM cte_holiday c
WHERE NOT EXISTS
(
    Select
        DateID
    From cte_absence a 
    WHERE c.UserID = a.UserID AND c.DateID = a.DateID)
