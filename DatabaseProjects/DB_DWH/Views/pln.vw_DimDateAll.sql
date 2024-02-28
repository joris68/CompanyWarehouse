


CREATE VIEW [pln].[vw_DimDateAll] AS 

    
  SELECT * FROM (
  SELECT 'Total' AS pParent , [YearID] AS pChild ,1 AS pWeight , 1 AS ORDERBY FROM [pln].[vw_DimDate] GROUP BY [YearID] 
  ) TY
  UNION
SELECT * FROM (
  SELECT  [YearID] as pParent  ,[QuarterID] AS pChild  ,1 AS pWeight , 2 AS ORDERBY FROM [pln].[vw_DimDateMonth]  GROUP BY [YearID]   ,[QuarterID]
  ) TQ
  UNION
SELECT * FROM (
  SELECT  [QuarterID] AS pParent  ,[MonthID] AS pChild , 1 as pWeight , 3 AS ORDERBY
  FROM [pln].[vw_DimDateMonth] GROUP BY [QuarterID]   ,[MonthID]
  ) TM

  UNION
SELECT * FROM (
  SELECT  [MonthID] as pParent ,[WeekID] AS pChild ,[Weight] AS pWeight, 4 AS ORDERBY   FROM [pln].[vw_DimDateWeek] 
  ) TW



