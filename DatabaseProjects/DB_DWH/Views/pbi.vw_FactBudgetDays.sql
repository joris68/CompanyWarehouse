CREATE view [pbi].[vw_FactBudgetDays] AS 

SELECT  
      [BlueAntProjectID]
      ,[BudgetWorkhours] / 8                        AS Projektbudget   
      ,[InvoiceWorkhours] / 8                       AS [Rechnungen (abgerechnet)]
      ,[BudgetInvoiceWorkhoursRemaining] / 8        AS [verbleibendes Projektbudget (abgerechnet)]   
      ,[BudgetInvoiceWorkhoursOpen] / 8             AS [Rechnungen (nicht abgerechnet)]
      ,[BudgetWorkhoursRemaining] / 8               AS [verbleibendes Projektbudget (nicht abgerechnet)]  
      ,[PlannedWorkhoursJira]/ 8                    AS [geplant in Jira]   
      ,[PlannedWorkhoursOpen] / 8                   AS [geplant in TM1]
      ,[BudgetWorkhoursFreeRemaining] / 8           AS [verbleibendes Projektbudget (ungeplant)]
      ,[BudgetWorkhoursFreeRemainingPlan] / 8       AS [verbleibendes Projektbudget (frei)]
FROM [pbi].[vw_FactProjectBudget]