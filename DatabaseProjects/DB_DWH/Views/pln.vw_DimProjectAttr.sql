


CREATE VIEW [pln].[vw_DimProjectAttr]
AS
SELECT b.ProjectNumber AS pElement, ProjectName AS pName, u.UserInitials AS pUserInitials, ProjectManagerID AS pProjectManagerID, CustomerID AS pCustomerID, StartDateID AS pStartDate, EndDateID AS pEnddate,
      [ProjectBudgetDays] AS pWorkPlannedDays, [ProjectWorktimeDays] AS pWorktimeDays, [ProjectRemainingDays] as pBudgetRemainingDays ,[ProjectBudgetEuro] AS pProjectBudgetEuro ,[ProjectInvoiceEuro] AS pProjectInvoiceEuro
      ,[ProjectRemainingEuro] AS pProjectRemainingEuro, NULL AS pBAID
FROM pln.vw_DimProject p LEFT JOIN pln.vw_DimUser u ON p.ProjectManagerID = u.UserID
LEFT JOIN [pln].[vw_FactProjectBudget] b ON p.[BlueAntProjectID] = b.[BlueAntProjectID]

 UNION 
      
SELECT 'de-' + RIGHT('00000' + CAST([DealID] AS VARCHAR(255)), 5)    AS pElement, [DealName] AS pName, '<N/A>' AS pUserInitials, -1 AS pProjectManagerID, -1 AS pCustomerID, -1 AS pStartDate, -1 AS pEnddate,
 -1 AS pWorkPlannedDays, -1 AS pWorktimeDays, 0 as pBudgetRemainingDays , -1 AS pProjectBudgetEuro , -1 AS pProjectInvoiceEuro, 0 AS pProjectRemainingEuro, NULL AS pBAID
FROM  [pln].[vw_DimDeal]

 UNION 
      
SELECT 'in-' + RIGHT('00000' + CAST([DealID] AS VARCHAR(255)), 5)    AS pElement, [DealName] AS pName, UserInitials AS pUserInitials, ProjectManagerID AS pProjectManagerID, -1 AS pCustomerID, -1 AS pStartDate, -1 AS pEnddate,
 -1 AS pWorkPlannedDays, -1 AS pWorktimeDays, 0 as pBudgetRemainingDays  , -1 AS pProjectBudgetEuro , -1 AS pProjectInvoiceEuro, 0 AS pProjectRemainingEuro, ProjectNumber AS pBAID
FROM  [pln].[vw_DimProjectInternal]
