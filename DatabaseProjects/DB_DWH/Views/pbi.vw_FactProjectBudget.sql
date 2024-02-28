CREATE VIEW [pbi].[vw_FactProjectBudget] AS 
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
                ,SUM(WorkPlannedMinutes) / 60  AS PlannedWorkhours
            FROM cte_taskplan
            GROUP BY BlueAntProjectID  ),

cte_invdate as (
                SELECT
                    MAX(DateID) AS DateID
                    ,[BlueAntProjectID]      
                FROM [pbi].[vw_FactWorklogSmall]
                GROUP BY BlueAntProjectID),

cte_worklog AS (
            SELECT 
                w.BlueAntProjectID
                ,SUM(BlueAntTimeMinutes) / 60  AS Workhours
                ,SUM([CostWorkhours])          AS CostWorkhours
            FROM [pbi].[vw_FactWorklogSmall] w            
            WHERE [Billable] = 1             
            GROUP BY w.BlueAntProjectID),


cte_invoice AS (
                SELECT 
                    SUM([InvoiceAmount])  AS Invoice
                    ,SUM(WorktimeAccountableInHours) AS InvoiceWorkhours
                    ,[BlueAntProjectID]
                FROM [dwh].[FactBlueAntInvoice]
                GROUP BY [BlueAntProjectID]),

cte_tm1 AS (
                SELECT  
                    p.[BlueAntProjectID]
                    ,SUM([PlannedWorkPerDay]) AS PlannedWorkHours
                    ,SUM([PlannedCostWorkDays]) AS PlannedCostWorkhours
                FROM [pbi].[vw_FactPlan] p 
                LEFT JOIN cte_invdate i   ON p.BlueAntProjectID = i.BlueAntProjectID 
                WHERE [Billable] = 1  AND p.DateID > i.DateID
                GROUP BY p.[BlueAntProjectID]),

cte_jira  AS (
                SELECT 
                    d.[BlueAntProjectID]
                    ,SUM([CurrentTimeEstimate])  AS PlannedWorkHours
                    ,SUM([CurrentTimeEstimateCost])          AS PlannedCostWorkhours
                FROM [pbi].[vw_DimJiraIssue] d
                LEFT JOIN [pbi].[vw_FactJiraIssue] f ON d.JiraIssueID = f.JiraIssueID
                WHERE CurrentTimeEstimate > 0
                GROUP BY [BlueAntProjectID])              
                            

SELECT              
      p.BlueAntProjectID
      ,cte_plan.PlannedWorkhours                                                                                                                        AS BudgetWorkhours
      ,InvoiceWorkhours                                                                                                                                 AS InvoiceWorkhours
      ,cte_plan.PlannedWorkhours -  ISNULL(cte_worklog.Workhours, 0)                                                                                    AS BudgetWorkhoursRemaining
      ,cte_plan.PlannedWorkhours - ISNULL(InvoiceWorkhours, 0)                                                                                          AS BudgetInvoiceWorkhoursRemaining
      ,cte_worklog.Workhours - ISNULL(InvoiceWorkhours, 0)                                                                                              AS BudgetInvoiceWorkhoursOpen
      ,b.BudgetValue                                                                                                                                    AS Budget
      ,Invoice                                                                                                                                          AS Invoice
      ,CostWorkhours - ISNULL(cte_invoice.Invoice, 0)                                                                                                   AS InvoiceOpen
      ,b.BudgetValue - ISNULL(cte_invoice.Invoice, 0)                                                                                                   AS BudgetInvoiceRemaining
      ,b.BudgetValue - ISNULL(CostWorkhours, 0)                                                                                                         AS BudgetInvoiceOpenRemaining
      ,ISNULL(cte_jira.PlannedWorkHours , 0)                                                                                                            AS PlannedWorkhoursJira
      ,ISNULL(cte_jira.PlannedCostWorkhours , 0)                                                                                                        AS PlannedCostWorkhoursJira
      ,ISNULL(cte_tm1.PlannedWorkHours, 0)                                                                                                              AS PlannedWorkhoursOpen
      ,ISNULL(cte_tm1.PlannedCostWorkhours, 0)                                                                                                          AS PlannedCostWorkhoursOpen
      ,(cte_plan.PlannedWorkhours - ISNULL(cte_worklog.Workhours, 0)) -  ISNULL(cte_jira.PlannedWorkHours, 0)                                           AS BudgetWorkhoursFreeRemaining
      ,(b.BudgetValue - ISNULL(CostWorkhours, 0)) -  ISNULL(cte_jira.PlannedCostWorkhours, 0)                                                           AS BudgetInvoiceFreeRemaining
      ,(cte_plan.PlannedWorkhours - ISNULL(cte_worklog.Workhours, 0)) -  ISNULL(cte_jira.PlannedWorkHours, 0) - ISNULL(cte_tm1.PlannedWorkHours, 0)     AS BudgetWorkhoursFreeRemainingPlan
      ,(b.BudgetValue - ISNULL(CostWorkhours, 0)) -  ISNULL(cte_jira.PlannedCostWorkhours, 0) - ISNULL(cte_tm1.PlannedCostWorkhours, 0)                 AS BudgetInvoiceFreeRemainingPlan
FROM pbi.vw_DimProject p
LEFT JOIN cte_plan ON p.BlueAntProjectID = cte_plan.BlueAntProjectID   
LEFT JOIN cte_worklog ON p.BlueAntProjectID = cte_worklog.BlueAntProjectID
LEFT JOIN cte_invoice ON cte_invoice.BlueAntProjectID =  p.BlueAntProjectID
LEFT JOIN cte_tm1 ON p.BlueAntProjectID = cte_tm1.BlueAntProjectID
LEFT JOIN cte_jira ON p.BlueAntProjectID = cte_jira.BlueAntProjectID
LEFT JOIN [dwh].[FactBlueAntBudget] b ON p.BlueAntProjectID = b.BlueAntProjectID