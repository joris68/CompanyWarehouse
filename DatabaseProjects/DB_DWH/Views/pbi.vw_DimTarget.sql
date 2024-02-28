

CREATE VIEW [pbi].[vw_DimTarget] AS 
with cte as 
( 
SELECT 
    [UserID]
    ,YearId
    ,COUNT([DateID]) AS Absence
  FROM [pbi].[vw_DimAbsence] 
  GROUP BY  [UserID], YearID 

  ),

cte_capa_basis as (

SELECT 
    [DateID]
    ,YearID
    ,[UserID]   
FROM [pbi].[vw_DimDate] d
INNER JOIN [pbi].[vw_FactTeamCapacity] c ON d.DateID BETWEEN c.[StartDateID] AND c.[EndDateID] AND d.WeekdayID = c.[Weekyday]



UNION

SELECT 
    [DateID]
    ,YearID
    ,[UserID]
FROM [pbi].[vw_DimDate] d
INNER JOIN [pbi].[vw_FactTeamExtendedCapacity] c ON d.DateID BETWEEN c.[StartDateID] AND c.[EndDateID] AND d.WeekdayID = c.[Weekyday]
),

cte_holiday as (

SELECT 
      c.UserID
      ,YearID
      ,c.DateID
FROM cte_capa_basis c
LEFT JOIN [pbi].[vw_FactNoWorking] h ON c.UserID = h.UserID AND c.DateID = h.HolidayDateID
WHERE  h.HolidayDateID IS NULL AND h.UserID IS NULL

),

cte1 AS
(SELECT
        [UserID]
        ,YearID
      ,COUNT([DateID]) AS Workdays
FROM cte_holiday
GROUP BY  [UserID] ,YearID),

ctegr AS (
select 
    cte1.UserID
    ,cte1.Workdays - ISNULL(cte.Absence, 0) AS Workdays
    ,cte1.YearID
from cte1
left join cte on cte1.UserID = cte.UserID AND cte1.YearID = cte.YearID)

SELECT 
      t.[YearID]
      ,t.[UserID]
      ,t.[Target]
      ,c.Workdays
      ,(t.[Target]/c.Workdays) * 8 AS TargetHours
  FROM  dwh.DimTarget t
  LEFT JOIN ctegr c ON t.UserID = c.UserID AND t.YearID = c.YearID
  
