


/****** Script for SelectTopNRows command from SSMS  ******/
CREATE View [pln].[vw_FactWorklog]
as
SELECT
	     CAST([CalendarYearID] AS VARCHAR(10)) + '-W' + [CalendarWeekText] AS Week
,[vw_FactWorklogDays].[ProjectID]
      ,[vw_FactWorklogDays].[UserID]
      ,[vw_FactWorklogDays].[Billable]
      ,SUM( [vw_FactWorklogDays].[LT] ) AS LT
  FROM [pln].[vw_FactWorklogDays]
  INNER JOIN dwh.DimDate ON [vw_FactWorklogDays].DateID = dwh.DimDate.DateID
  GROUP BY
  	     CAST([CalendarYearID] AS VARCHAR(10)) + '-W' + [CalendarWeekText]
  ,[vw_FactWorklogDays].[ProjectID]
      ,[vw_FactWorklogDays].[UserID]
      ,[vw_FactWorklogDays].[Billable]


