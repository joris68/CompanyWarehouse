CREATE VIEW [pbi].[vw_FactBlueAntWorkTimeCalendarDetail] AS 

SELECT BlueAntWorkTimeCalendarDetailID, Weekyday, Worktime 
FROM

( SELECT 
    [BlueAntWorkTimeCalendarDetailID]
      ,[Monday] AS "1"
      ,[Tuesday] AS "2"
      ,[Wednesday] AS "3"
      ,[Thursday] AS "4"
      ,[Friday] AS "5"
  FROM [dwh].[DimBlueAntWorkTimeCalendarDetail] ) p
  UNPIVOT
    ( Worktime FOR Weekyday IN 
        ("1", "2", "3", "4", "5")
        )  AS unpvt;
