CREATE VIEW pbi.vw_DimAccount AS 
	SELECT 
		[AccountId]				AS AccountId
		,[Account]				AS Account
		,[AccountName]			AS AccountName
		,[IsLeaf]				AS IsLeaf
		,AccountReportingName	AS AccountReportingName
	FROM mart.[DimAccount]
