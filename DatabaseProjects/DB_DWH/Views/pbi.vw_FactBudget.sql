CREATE view [pbi].[vw_FactBudget] AS 
SELECT  
    [BlueAntProjectID]      
    ,[Budget]                                           AS Projektbudget                    
    ,[Invoice]                                          AS [Rechnungen (abgerechnet)]
    ,[BudgetInvoiceRemaining]                           AS [verbleibendes Projektbudget (abgerechnet)]
    ,[InvoiceOpen]                                      AS [Rechnungen (nicht abgerechnet)]
    ,[BudgetInvoiceOpenRemaining]                       AS [verbleibendes Projektbudget (nicht abgerechnet)]   
    ,[PlannedCostWorkhoursJira]                         AS [geplant in Jira]
    ,[PlannedCostWorkhoursOpen]                         AS [geplant in TM1]
    ,[BudgetInvoiceFreeRemaining]                       AS [verbleibendes Projektbudget (ungeplant)]
    ,[BudgetInvoiceFreeRemainingPlan]                   AS [verbleibendes Projektbudget (frei)]
FROM [pbi].[vw_FactProjectBudget]