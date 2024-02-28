CREATE PROCEDURE [etl].[prc_FillMartFactBusinessManagementEvaluationPlan] AS
BEGIN
	
	DROP TABLE IF EXISTS #tabTmp;
	DROP TABLE IF EXISTS #tabTmpAccounts;

	;WITH CTE_Prep AS
	(
		SELECT 
			 PARSE(NULLIF(a.Januar	,'')		AS DECIMAL(18,2) USING 'de-DE')	AS Januar
			,PARSE(NULLIF(a.Februar	,'')		AS DECIMAL(18,2) USING 'de-DE')	AS Februar
			,PARSE(NULLIF(a.März		,'')	AS DECIMAL(18,2) USING 'de-DE')	AS März
			,PARSE(NULLIF(a.April		,'')	AS DECIMAL(18,2) USING 'de-DE')	AS April
			,PARSE(NULLIF(a.Mai		,'')		AS DECIMAL(18,2) USING 'de-DE')	AS Mai
			,PARSE(NULLIF(a.Juni		,'')	AS DECIMAL(18,2) USING 'de-DE')	AS Juni
			,PARSE(NULLIF(a.Juli		,'')	AS DECIMAL(18,2) USING 'de-DE')	AS Juli
			,PARSE(NULLIF(a.August	,'')		AS DECIMAL(18,2) USING 'de-DE')	AS August
			,PARSE(NULLIF(a.September	,'')	AS DECIMAL(18,2) USING 'de-DE')	AS September
			,PARSE(NULLIF(a.Oktober	,'')		AS DECIMAL(18,2) USING 'de-DE')	AS Oktober
			,PARSE(NULLIF(a.November	,'')	AS DECIMAL(18,2) USING 'de-DE')	AS November
			,PARSE(NULLIF(a.Dezember	,'')	AS DECIMAL(18,2) USING 'de-DE')	AS Dezember	
			,LTRIM(RTRIM(a.Bezeichnung))		AS Bezeichnung
			,ISNULL(b.[AccountId],-1)			AS AccountId
		FROM [tmp].[Soll_DBR_DEV_20240221] a
			LEFT JOIN mart.[DimAccount] b on LTRIM(RTRIM(a.Bezeichnung)) = b.[AccountReportingName]
		WHERE LTRIM(RTRIM(a.Bezeichnung)) IN --DBR 20240222 Entscheidung DBR nur die Konto zu nehmen die wirklich einen Wert haben, alle anderen sind aktuell Summen
		(
			'UE Dienstleistung'
			,'UE Software'
			,'WE Dienstleistung'
			,'WE Software'
			,'So. betr. Erlöse'
			,'Personalkosten'
			,'Raumkosten'
			,'Versich./Beiträge'
			,'Rep./Instandh.'
			,'Kfz-Kosten o.St.'
			,'Werbekosten'
			,'Reisekosten'
			,'Kosten Warenabg'
			,'Verwaltungskosten'
			,'Fortbildung'
			,'Beratungskosten'
			,'Leasing und Miete'
			,'Geldverkehr'
			,'Sonstige Kosten'
			,'Abschreibungen'
			,'Zinsaufwand'
			,'Sonst neutr Aufw'
		)

	)
	,CTE AS
	(
		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.Januar AS Balance
			,20240101 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240101')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 

		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.Februar AS Balance
			,20240201 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240201')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 

		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.März AS Balance
			,20240301 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240301')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	
		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.April AS Balance
			,20240401 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240401')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	

		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.Mai AS Balance
			,20240501 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240501')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	
		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.Juni AS Balance
			,20240601 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240601')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	
		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.Juli AS Balance
			,20240701 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240701')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	
		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.August AS Balance
			,20240801 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240801')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	
		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.September AS Balance
			,20240901 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20240901')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 

		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.Oktober AS Balance
			,20241001 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20241001')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	
		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.November AS Balance
			,20241101 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20241101')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
		
		UNION ALL

		SELECT 
			a.AccountId
			,a.Bezeichnung
			,a.Dezember AS Balance
			,20241201 AS DateId
			,CONVERT(NVARCHAR(500),CONCAT(a.Bezeichnung,'|20241201')) AS PlanningTransactionIdentifier
		FROM CTE_Prep a 
	)


	SELECT *
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(PlanningTransactionIdentifier,'N/A')),
                            '|'--,CONVERT(VARCHAR(30), ISNULL(KindAccountId,-1))
                            --'|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyBK
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), AccountId,-1), --NULL Behandlung in Qry
                            '|',CONVERT(VARCHAR(30), DateId), --NULL Behandlung in Qry
                            '|',CONVERT(VARCHAR(30), ISNULL(Balance,'-1'))
                        )
                    ), 2)) AS HashKeyValue
	INTO #tabTmp
	FROM CTE

	SELECT 
		Bezeichnung AS ErrorTransactionIdentifier
		,0 AS IsDeleted
	INTO #tabTmpAccounts
	fROM #tabTmp
	WHERE AccountId = -1
	GROUP BY Bezeichnung


	--GetFacts
	MERGE mart.[FactBusinessManagementEvaluationPlan] AS dst 
	USING #tabTmp AS src
	ON dst.HashKeyBK = src.HashKeyBK
	WHEN MATCHED 
		AND dst.HashKeyValue <> src.HashKeyValue 
	THEN UPDATE SET 
		UpdateDate = GETDATE()
		,HashKeyValue = src.HashKeyValue 
		,IsDeleted = 0
		,DeleteDate = NULL


	WHEN NOT MATCHED BY SOURCE
		AND IsDeleted = 0 
	THEN UPDATE SET
		IsDeleted = 1
		,DeleteDate = GETDATE()
		,HashkeyValue = 'N/A'

	WHEN NOT MATCHED BY TARGET 
	THEN INSERT
	(
		PlanningTransactionIdentifier
		,AccountId
		,[DateId]
		,Balance
		,[InsertDate]	
		,[UpdateDate]	
		,[HashkeyBK]    
		,[HashkeyValue] 
		,IsDeleted 
		,DeleteDate

	)
	VALUES
	(
		[PlanningTransactionIdentifier]
		,[AccountId]
		,[DateId]
		,Balance
		,GETDATE()
		,GETDATE()
		,[HashkeyBK]    
		,[HashkeyValue] 
		,0
		,GETDATE()
	);


	--SolveErrors
	MERGE error.[mart_FactBusinessManagementEvaluationPlanMissingAccountMapping] AS dst 
	USING #tabTmpAccounts AS src
	ON dst.ErrorTransactionIdentifier = src.ErrorTransactionIdentifier
	WHEN MATCHED 
		AND src.IsDeleted != dst.IsDeleted
	THEN UPDATE SET 
		UpdateDate = GETDATE()
		,IsDeleted = 0
		,DeleteDate = NULL
	WHEN NOT MATCHED BY SOURCE
		AND dst.IsDeleted = 0 
	THEN UPDATE SET
		IsDeleted = 1
		,DeleteDate = GETDATE()

	WHEN NOT MATCHED BY TARGET 
	THEN INSERT
	(
		ErrorTransactionIdentifier
		,IsDeleted
		,[InsertDate]	
		,[UpdateDate]	
		

	)
	VALUES
	(
		ErrorTransactionIdentifier
		,IsDeleted
		,GETDATE()
		,GETDATE()
		
	)


	;
END