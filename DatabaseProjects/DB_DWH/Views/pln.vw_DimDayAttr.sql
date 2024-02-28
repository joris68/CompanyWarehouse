
CREATE VIEW [pln].[vw_DimDayAttr]
AS
  SELECT * FROM (
  SELECT [YearID] AS pElement ,'' AS [DateNum],'' AS [DateText],'' AS [DayID],'' AS [DayText],'' AS [MonthNum],'' AS [MonthText],'' AS [QuarterNum],'' AS [QuarterText],'' AS [CalendarWeekText],
  '' AS [CalendarWeekYearText],  [YearID] AS [YearNum],'' AS [MonthNameDE],'' AS [MonthShortNameDE],'' AS [MonthNameEN],'' AS [MonthShortNameEN],'' AS [WeekdayID],'' AS [WeekdayNameDE],
  '' AS [WeekdayShortNameDE],'' AS [WeekdayNameEN],'' AS [WeekdayShortNameEN],'' AS [DayOfYear],'' AS [YearMonthNum],'' AS [IsEndOfMonth],'' AS [IsWeekend], MIN(DateID) AS [FirstDay],
  MAX(DateID) AS [LastDay], SUM(1) AS [DaysIn], '' AS IndexNum 
 , 1 AS ORDERBY FROM [pln].[vw_DimDay]   GROUP BY [YearID] -- ORDER BY [YearID]
      ) TY

  UNION


SELECT * FROM (
  SELECT [QuarterID] AS pElement ,'' AS [DateNum],'' AS [DateText],'' AS [DayID],'' AS [DayText],'' AS [MonthNum],'' AS [MonthText],'' AS [QuarterNum],'' AS [QuarterText],'' AS [CalendarWeekText],
  '' AS [CalendarWeekYearText],  MAX([YearID]) AS [YearNum],'' AS [MonthNameDE],'' AS [MonthShortNameDE],'' AS [MonthNameEN],'' AS [MonthShortNameEN],'' AS [WeekdayID],'' AS [WeekdayNameDE],
  '' AS [WeekdayShortNameDE],'' AS [WeekdayNameEN],'' AS [WeekdayShortNameEN],'' AS [DayOfYear],'' AS [YearMonthNum],'' AS [IsEndOfMonth],'' AS [IsWeekend], MIN(DateID) AS [FirstDay],
  MAX(DateID) AS [LastDay], SUM(1) AS [DaysIn], '' AS IndexNum 
 , 2 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [QuarterID]   -- ORDER BY [YearID]
      ) TQ


   UNION

   SELECT * FROM (
     SELECT [MonthID] AS pElement ,'' AS [DateNum],'' AS [DateText],'' AS [DayID],'' AS [DayText],
	 MAX([MonthNum]) AS [MonthNum],MAX([MonthText])  AS [MonthText],MAX([QuarterNum])  AS [QuarterNum],MAX([QuarterText])  AS [QuarterText],'' AS [CalendarWeekText],
  '' AS [CalendarWeekYearText],  Max([YearID]) AS [YearNum],  
  MAX([MonthNameDE]) AS [MonthNameDE], MAX([MonthShortNameDE]) AS [MonthShortNameDE],  MAX([MonthNameEN]) AS [MonthNameEN], MAX( [MonthShortNameEN]) AS [MonthShortNameEN],'' AS [WeekdayID],'' AS [WeekdayNameDE],
  '' AS [WeekdayShortNameDE],'' AS [WeekdayNameEN],'' AS [WeekdayShortNameEN],'' AS [DayOfYear], MAX( [YearMonthNum]) AS [YearMonthNum],'' AS [IsEndOfMonth],'' AS [IsWeekend], MIN(DateID) AS [FirstDay],
  MAX(DateID) AS [LastDay], SUM(1) AS [DaysIn], '' AS IndexNum 
 , 3 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [MonthID]   -- ORDER BY [YearID]
  ) TM

   UNION


      SELECT * FROM (
     SELECT [CalendarWeekID] AS pElement ,'' AS [DateNum],'' AS [DateText],'' AS [DayID],'' AS [DayText],
	 MAX([MonthNum]) AS [MonthNum],MAX([MonthText])  AS [MonthText],MAX([QuarterNum])  AS [QuarterNum],MAX([QuarterText])  AS [QuarterText],
	 MAX([CalendarWeekText]) AS [CalendarWeekText],
 MAX([CalendarWeekYearText]) AS [CalendarWeekYearText],  Max([YearID]) AS [YearNum],  
  MAX([MonthNameDE]) AS [MonthNameDE], MAX([MonthShortNameDE]) AS [MonthShortNameDE],  MAX([MonthNameEN]) AS [MonthNameEN], MAX( [MonthShortNameEN]) AS [MonthShortNameEN],'' AS [WeekdayID],'' AS [WeekdayNameDE],
  '' AS [WeekdayShortNameDE],'' AS [WeekdayNameEN],'' AS [WeekdayShortNameEN],'' AS [DayOfYear], MAX( [YearMonthNum]) AS [YearMonthNum],'' AS [IsEndOfMonth],'' AS [IsWeekend], MIN(DateID) AS [FirstDay],
  MAX(DateID) AS [LastDay], SUM(1) AS [DaysIn], '' AS IndexNum 
 , 4 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [CalendarWeekID]   -- ORDER BY [YearID]
  ) TW


  UNION


  
      SELECT * FROM (
     SELECT [DateID] AS pElement ,
	 MAX([DateNum]) AS [DateNum],MAX([DateText])  AS [DateText],MAX([DayID])  AS [DayID],MAX([DayText])  AS [DayText],
	 MAX([MonthNum]) AS [MonthNum],MAX([MonthText])  AS [MonthText],MAX([QuarterNum])  AS [QuarterNum],MAX([QuarterText])  AS [QuarterText],
	 MAX([CalendarWeekText]) AS [CalendarWeekText],
 MAX([CalendarWeekYearText]) AS [CalendarWeekYearText],  Max([YearID]) AS [YearNum],  
  MAX([MonthNameDE]) AS [MonthNameDE], MAX([MonthShortNameDE]) AS [MonthShortNameDE],  MAX([MonthNameEN]) AS [MonthNameEN], MAX( [MonthShortNameEN]) AS [MonthShortNameEN],
  MAX([WeekdayID])  AS [WeekdayID],MAX([WeekdayNameDE])  AS [WeekdayNameDE],MAX([WeekdayShortNameDE])  AS [WeekdayShortNameDE],MAX([WeekdayNameEN])  AS [WeekdayNameEN],MAX([WeekdayShortNameEN])  AS [WeekdayShortNameEN],MAX([DayOfYear])  AS [DayOfYear],
  MAX( [YearMonthNum]) AS [YearMonthNum],MAX([IsEndOfMonth]) AS [IsEndOfMonth],MAX([IsWeekend]) AS [IsWeekend], MIN(DateID) AS [FirstDay],
  MAX(DateID) AS [LastDay], SUM(1) AS [DaysIn], '' AS IndexNum 
 , 5 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [DateID]   -- only for consistency!
  )  TD


  UNION


SELECT * FROM (
  SELECT REPLACE([QuarterID],'-','.')  AS pElement ,'' AS [DateNum],'' AS [DateText],'' AS [DayID],'' AS [DayText],'' AS [MonthNum],'' AS [MonthText],'' AS [QuarterNum],'' AS [QuarterText],'' AS [CalendarWeekText],
  '' AS [CalendarWeekYearText],  MAX([YearID]) AS [YearNum],'' AS [MonthNameDE],'' AS [MonthShortNameDE],'' AS [MonthNameEN],'' AS [MonthShortNameEN],'' AS [WeekdayID],'' AS [WeekdayNameDE],
  '' AS [WeekdayShortNameDE],'' AS [WeekdayNameEN],'' AS [WeekdayShortNameEN],'' AS [DayOfYear],'' AS [YearMonthNum],'' AS [IsEndOfMonth],'' AS [IsWeekend], MIN(DateID) AS [FirstDay],
  MAX(DateID) AS [LastDay], SUM(1) AS [DaysIn], '' AS IndexNum 
 , 6 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [QuarterID]   -- ORDER BY [YearID]
      ) TQP


   UNION

   SELECT * FROM (
     SELECT REPLACE([MonthID],'-','.') AS pElement ,'' AS [DateNum],'' AS [DateText],'' AS [DayID],'' AS [DayText],
	 MAX([MonthNum]) AS [MonthNum],MAX([MonthText])  AS [MonthText],MAX([QuarterNum])  AS [QuarterNum],MAX([QuarterText])  AS [QuarterText],'' AS [CalendarWeekText],
  '' AS [CalendarWeekYearText],  Max([YearID]) AS [YearNum],  
  MAX([MonthNameDE]) AS [MonthNameDE], MAX([MonthShortNameDE]) AS [MonthShortNameDE],  MAX([MonthNameEN]) AS [MonthNameEN], MAX( [MonthShortNameEN]) AS [MonthShortNameEN],'' AS [WeekdayID],'' AS [WeekdayNameDE],
  '' AS [WeekdayShortNameDE],'' AS [WeekdayNameEN],'' AS [WeekdayShortNameEN],'' AS [DayOfYear], MAX( [YearMonthNum]) AS [YearMonthNum],'' AS [IsEndOfMonth],'' AS [IsWeekend], MIN(DateID) AS [FirstDay],
  MAX(DateID) AS [LastDay], SUM(1) AS [DaysIn], '' AS IndexNum 
 , 7 AS ORDERBY FROM [pln].[vw_DimDay]  GROUP BY [MonthID]   
  ) TMP


