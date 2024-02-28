CREATE VIEW [pbi].[vw_FactBusinessManagementEvaluation] AS
	SELECT 
		DateId			AS DateId
		,Balance			AS Balance
		,AccountId		AS AccountId
	FROM mart.[FactBusinessManagementEvaluation]
	WHERE IsDeleted = 0