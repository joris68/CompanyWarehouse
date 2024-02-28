CREATE VIEW [pbi].[vw_FactBusinessManagementEvaluationPlanMissingAccountMapping] AS 
	SELECT 
		ErrorTransactionIdentifier AS AccountReportingName
	FROM error.[mart_FactBusinessManagementEvaluationPlanMissingAccountMapping]
	WHERE IsDeleted = 0
