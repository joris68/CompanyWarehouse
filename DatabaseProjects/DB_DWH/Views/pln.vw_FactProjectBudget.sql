CREATE VIEW [pln].[vw_FactProjectBudget] AS 


with cte_taskplan as (
                        SELECT DISTINCT
                                ft.BlueAntProjectTaskID
                                ,dt.BlueAntProjectID
                                ,ft.WorkPlannedMinutes
                        FROM dwh.DimBlueAntProjectTask dt    
                        LEFT JOIN dwh.FactBlueAntProjectTask ft ON ft.BlueAntProjectTaskID = dt.BlueAntProjectTaskID
                        WHERE dt.BlueAntTaskBK NOT IN (
                                                        SELECT DISTINCT
                                                            TaskParent
                                                        FROM
                                                            dwh.DimBlueAntProjectTask
                                                        WHERE TaskParent != -1)),

cte_plan AS (
            select 
                BlueAntProjectID
                ,SUM(WorkPlannedMinutes) / 60 / 8 AS WorkPlannedDays
            FROM cte_taskplan
            GROUP BY BlueAntProjectID  ),

cte_worklog AS (
                SELECT 
                BlueAntProjectID
                ,SUM(BlueAntTimeSpentMinute) / 60 / 8 AS Days
            FROM dwh.FactBlueAntWorklog
            WHERE Billable = 1
            GROUP BY BlueAntProjectID),

cte_workloghistory AS (
            SELECT 
                BlueAntProjectID
                ,SUM(BlueAntTimeSpentMinute) / 60 / 8 AS Days
            FROM dwh.FactBlueAntWorklogHistory
            WHERE Billable = 1
            GROUP BY BlueAntProjectID),

cte_invoice AS (
                SELECT 
                    SUM([InvoiceAmount])  AS ProjectInvoiceEuro
                    ,[BlueAntProjectID]
                FROM [dwh].[FactBlueAntInvoice]
                GROUP BY [BlueAntProjectID])            

SELECT              
      pln.vw_DimProject.BlueAntProjectID
      ,pln.vw_DimProject.ProjectNumber
      ,WorkPlannedDays AS ProjectBudgetDays
      ,cte_worklog.Days + ISNULL(cte_workloghistory.[Days], 0) AS ProjectWorktimeDays
      ,WorkPlannedDays - ISNULL(cte_worklog.Days, 0) - ISNULL(cte_workloghistory.[Days], 0) AS ProjectRemainingDays
      ,b.BudgetValue AS ProjectBudgetEuro
      ,cte_invoice.ProjectInvoiceEuro
      ,b.BudgetValue - ISNULL(cte_invoice.ProjectInvoiceEuro, 0) AS ProjectRemainingEuro
FROM pln.vw_DimProject
LEFT JOIN cte_plan ON pln.vw_DimProject.BlueAntProjectID = cte_plan.BlueAntProjectID   
LEFT JOIN cte_worklog ON pln.vw_DimProject.BlueAntProjectID = cte_worklog.BlueAntProjectID   
LEFT JOIN cte_workloghistory ON cte_workloghistory.BlueAntProjectID =  pln.vw_DimProject.BlueAntProjectID
LEFT JOIN cte_invoice ON cte_invoice.BlueAntProjectID =  pln.vw_DimProject.BlueAntProjectID   
LEFT JOIN [dwh].[FactBlueAntBudget] b ON pln.vw_DimProject.BlueAntProjectID = b.BlueAntProjectID




