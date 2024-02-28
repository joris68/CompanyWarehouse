CREATE VIEW [pbi].[vw_FactNoWorking] as 

SELECT  U.[UserID]
      ,[HolidayDateID]
  FROM [tmp].[dwh_DimUserExtended] S
  LEFT JOIN dwh.DimUser U ON S.BlueAntUserBK = U.BlueAntUserBK
  LEFT JOIN [dwh].[DimHolidays] H ON S.State = H.[State]

     
