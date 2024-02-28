CREATE PROCEDURE [etl].[prc_FillMartDimAccount] AS
BEGIN

	DROP TABLE IF EXISTS #tabTmp; 

	;WITH CTE AS
	(
		SELECT 
			LTRIM(RTRIM(NULLIF(Konto,''))) Account
			,LTRIM(RTRIM(NULLIF(Zeilenbeschriftung,''))) AccountName
			,CONVERT(TINYINT,IIF(FktSchl = 0,1,0)) AS IsLeaf
		FROM [stg].[datevBWA]
		--WHERE CONVERT(SMALLINT,NULLIF(FktSchl,'')) = 0 --20240221 Annahme DBR das sind meine Konten

		UNION 

		SELECT 
			LTRIM(RTRIM(NULLIF(Zeile2,''))) Account
			,LTRIM(RTRIM(NULLIF(Zeilenbeschriftung,''))) AccountName
			,CONVERT(TINYINT,0) AS IsLeaf
		FROM [stg].[datevBWA]
		--WHERE CONVERT(SMALLINT,NULLIF(FktSchl,'')) in 
		--		(
		--			1 --20240221 Annahme DBR das ist meine erste Ebene
		--		)
	
		UNION 

		SELECT 
			LTRIM(RTRIM(NULLIF(Zeile1,''))) Account
			,LTRIM(RTRIM(NULLIF(Bezeichnung,''))) AccountName
			,CONVERT(TINYINT,0) AS IsLeaf
		FROM [stg].[datevBWA]
		--WHERE CONVERT(SMALLINT,NULLIF(FktSchl,'')) in 
		--		(
		--			2 --20240221 Annahme DBR das ist meine zweite Ebene
		--		)	

	)

	SELECT 
		Account
		,MIN(AccountName)		AS AccountName
		,MIN(IsLeaf)	AS IsLeaf
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(Account,'-1')),
                            '|'--,CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
							--'|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyBK
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(MIN(AccountName),'N/A')),
                            '|',CONVERT(VARCHAR(30), ISNULL(MIN(IsLeaf),0))--, --tinyint
                            --'|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyValue

	INTO #tabTmp
	FROM CTE
	WHERE Account is not null
	GROUP BY Account
	HAVING COUNT(DISTINCT AccountName) = 1  --ErrorCounter


	MERGE mart.[DimAccount] AS dst 
	USING #tabTmp AS src
	ON dst.HashKeyBK = src.HashKeyBK
	WHEN MATCHED 
		AND dst.HashKeyValue <> src.HashKeyValue 
	THEN UPDATE SET 
		[AccountName] = src.AccountName
		,UpdateDate = GETDATE()
		,HashKeyValue = src.HashKeyValue 
		,[IsLeaf] = src.IsLeaf

	WHEN NOT MATCHED BY TARGET 
	THEN INSERT
	(
		[Account]		
		,[AccountName]
		,AccountReportingName
		,[InsertDate]	
		,[UpdateDate]	
		,[HashkeyBK]    
		,[HashkeyValue] 
		,[IsLeaf]
	)
	VALUES
	(
		[Account]		
		,[AccountName] 
		,N'N/A'			--DBR 20240222 manuelles Attribut deswegen auch nicht im Hashkeyvalue
		,GETDATE()
		,GETDATE()
		,[HashkeyBK]    
		,[HashkeyValue] 
		,IsLeaf
	)

	;

END
