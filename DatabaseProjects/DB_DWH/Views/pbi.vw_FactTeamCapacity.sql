CREATE VIEW [pbi].[vw_FactTeamCapacity] AS 

with cte_worktime as (

    SELECT  
      [BlueAntUserID]
      ,[WorkTimeCalendarID]
      ,[StartDateID]
      ,[EndDateID]
      ,[DateOfLeavingID]
      ,HashkeyBK
  FROM [dwh].[FactBlueAntWorktimeCalendar]

UNION

    SELECT  
      -2 AS [BlueAntUserID]
      ,2 AS [WorkTimeCalendarID]
      ,20230101 AS [StartDateID]
      ,20241231 AS [EndDateID]
      ,99991231 AS [DateOfLeavingID]
      ,'N/A' AS HashkeyBK
)

SELECT u.UserID
      ,[StartDateID]
      ,[EndDateID]
      ,[Weekyday]
      ,[Worktime] / 60 AS WorktimeHours
FROM cte_worktime c
LEFT JOIN [pbi].[vw_FactBlueAntWorkTimeCalendarDetail] cd ON c.WorkTimeCalendarID = cd.BlueAntWorkTimeCalendarDetailID
LEFT JOIN [dwh].[DimBlueAntUser] bu ON c.BlueAntUserID = bu.[BlueAntUserID]
LEFT JOIN [dwh].[DimUser] u ON bu.BlueAntUserBK = u.[BlueAntUserBK]
LEFT JOIN [tmp].[dwh_DimUserExtended] fT ON U.BlueAntUserBK = fT.BlueAntUserBK
WHERE ft.TeamName IN ('Team Berlin','Team Münster') AND  c.HashkeyBK != '1f042e55accb28513c420244bb8322cd67df89aeffad8441e1f23c1e103b64e1' 