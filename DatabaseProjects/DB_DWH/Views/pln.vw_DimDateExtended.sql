

/****** Script for SelectTopNRows command from SSMS  ******/
CREATE VIEW [pln].[vw_DimDateExtended]
AS
SELECT  
CAST(YearID AS VARCHAR(255)) AS YearID, 
CAST(YearID AS VARCHAR(255)) + '-' + QuarterText AS QuarterID, 
CAST(YearID AS VARCHAR(255)) + '-' + MonthText AS MonthID, 
CAST(CalendarYearID AS VARCHAR(255)) + '-W' + CalendarWeekText AS WeekID, 
(CASE WHEN [IsWeekend] = 1 THEN 0 ELSE 1 END) AS Workdays, 
(1) AS DaysCount, 
MonthID AS MonthNr, 
MonthNameDE, 
MonthShortNameDE, 
MonthNameEN, 
MonthShortNameEN, 
QuarterID AS QuarterNr, 
QuarterText, CalendarWeekText, 
LastDayOfMonth, DaysInMonth, 
CAST(FirstDayOfWeek AS VARCHAR(255)) AS FirstDayOfWeek, 
CAST(LastDayOfWeek AS VARCHAR(255)) AS LastDayOfWeek,

CONVERT(VARCHAR(30),  DATEADD(day,  1, CONVERT(DATETIME,[DateText],103) ), 23) AS DayNext,
CONVERT(VARCHAR(30),  DATEADD(day, -1, CONVERT(DATETIME,[DateText],103) ) , 23) AS DayPrev,

FORMAT( DATEADD(month,  1, CONVERT(DATETIME,[DateText],103) ),'yyyy-MM') AS MonthNext,
FORMAT( DATEADD(month, -1, CONVERT(DATETIME,[DateText],103) ),'yyyy-MM') AS MonthPrev,

FORMAT( DATEADD(YEAR,  1, CONVERT(DATETIME,[DateText],103) ),'yyyy') AS YearNext,
FORMAT( DATEADD(YEAR, -1, CONVERT(DATETIME,[DateText],103) ),'yyyy') AS YearPrev,

DATEPART(iso_week,  DATEADD(DAY,  7, CONVERT(DATETIME,[DateText],103) ) ) AS CalenderWeekIDNext,
DATEPART(iso_week,  DATEADD(DAY, -7, CONVERT(DATETIME,[DateText],103) ) ) AS CalenderWeekIDPrev,

FORMAT( DATEADD(DAY,  7, CONVERT(DATETIME,[DateText],103) ) ,'yyyy-W') + FORMAT( DATEPART(iso_week,  DATEADD(DAY,  7, CONVERT(DATETIME,[DateText],103) ) ), '00' ) AS WeekNext, 
FORMAT( DATEADD(DAY, -7, CONVERT(DATETIME,[DateText],103) ) ,'yyyy-W') + FORMAT( DATEPART(iso_week,  DATEADD(DAY, -7, CONVERT(DATETIME,[DateText],103) ) ), '00' ) AS WeekPrev,

FORMAT(  DATEADD(QUARTER,  1, CONVERT(DATETIME,[DateText],103) ), 'yyyy-Q') + FORMAT(  DATEPART(QUARTER,  DATEADD(QUARTER,  1, CONVERT(DATETIME,[DateText],103) ) ), '0') AS QuarterNext,
FORMAT(  DATEADD(QUARTER, -1, CONVERT(DATETIME,[DateText],103) ), 'yyyy-Q') + FORMAT(  DATEPART(QUARTER,  DATEADD(QUARTER, -1, CONVERT(DATETIME,[DateText],103) ) ), '0') AS QuarterPrev
FROM 
 dwh.DimDate
WHERE        (YearID >= 2022)
