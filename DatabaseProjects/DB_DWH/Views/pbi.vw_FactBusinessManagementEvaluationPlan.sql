CREATE VIEW [pbi].[vw_FactBusinessManagementEvaluationPlan] AS 
	SELECT 	DateId				AS DateId
			,Balance				AS Balance
			,AccountId 			AS AccountId
	FROM mart.[FactBusinessManagementEvaluationPlan]
	WHERE IsDeleted = 0 
