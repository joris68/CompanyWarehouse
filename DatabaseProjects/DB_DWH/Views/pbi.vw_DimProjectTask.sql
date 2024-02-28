CREATE view [pbi].[vw_DimProjectTask]
as
with cte as 
(
SELECT DISTINCT
    ft.BlueAntProjectTaskID
    ,ft.WorkPlannedMinutes
FROM
    dwh.DimBlueAntProjectTask dt    
LEFT JOIN dwh.FactBlueAntProjectTask ft ON ft.BlueAntProjectTaskID = dt.BlueAntProjectTaskID
WHERE dt.BlueAntTaskBK NOT IN (
    SELECT DISTINCT
        TaskParent
    FROM
        dwh.DimBlueAntProjectTask
    WHERE TaskParent != -1)
)

SELECT
     dt.BlueAntProjectTaskID
    ,CASE WHEN dt2.BlueAntProjectTaskID = -1 THEN  NULL
     ELSE   dt2.BlueAntProjectTaskID  
     END AS BlueAntProjectTaskParentID
    ,dt.BlueAntProjectID
    ,dt.TaskName
    ,cte.WorkPlannedMinutes / 60 AS WorkPlannedHours
FROM dwh.DimBlueAntProjectTask dt
LEFT JOIN dwh.DimBlueAntProjectTask dt2 ON dt.TaskParent = dt2.BlueAntTaskBK    
LEFT JOIN cte  ON cte.BlueAntProjectTaskID = dt.BlueAntProjectTaskID