



CREATE VIEW [pln].[vw_DimDayAll] AS 

    
    SELECT * FROM (
  SELECT 'Total' AS pParent , [YearID] AS pChild ,1 AS pWeight , 1 AS ORDERBY FROM [pln].[vw_DimDay] GROUP BY [YearID] 
  ) TY

  UNION
SELECT * FROM (
  SELECT  [YearID] as pParent  ,[QuarterID] AS pChild  ,1 AS pWeight , 2 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [YearID]   ,[QuarterID]
  ) TQ
   UNION
SELECT * FROM (
  SELECT  [QuarterID] AS pParent  ,[MonthID] AS pChild , 1 as pWeight , 3 AS ORDERBY
  FROM [pln].[vw_DimDay] GROUP BY [QuarterID]   ,[MonthID]
  ) TM
   UNION
SELECT * FROM (
  SELECT  [MonthID] as pParent ,[WeekID] AS pChild ,[Weight] AS pWeight, 4 AS ORDERBY   FROM [pln].[vw_DimDateWeek] 
  ) TW
  UNION
  SELECT * FROM (
  SELECT  CalendarWeekID AS pParent , [DateID] AS pChild ,1 AS pWeight , 5 AS ORDERBY FROM [pln].[vw_DimDay]   
  ) TD
  UNION

    SELECT * FROM (
  SELECT 'Total.' AS pParent , [YearID] + '.' AS pChild ,1 AS pWeight , 6 AS ORDERBY FROM [pln].[vw_DimDay] GROUP BY [YearID] 
  ) TYM

  UNION

SELECT * FROM (
  SELECT  [YearID] + '.' as pParent  , REPLACE([QuarterID],'-','.')  AS pChild  ,1 AS pWeight , 7 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [YearID]   ,[QuarterID]
  ) TQM

  UNION

SELECT * FROM (
  SELECT  REPLACE([QuarterID],'-','.') AS pParent , REPLACE([MonthID],'-','.') AS pChild , 1 as pWeight , 8 AS ORDERBY
  FROM [pln].[vw_DimDay] GROUP BY [QuarterID]   , [MonthID]
  ) TMM

  UNION

  SELECT * FROM (
  SELECT REPLACE([MonthID],'-','.')   AS pParent , [DateID] AS pChild ,1 AS pWeight , 9 AS ORDERBY FROM [pln].[vw_DimDay]   
  ) TDM


