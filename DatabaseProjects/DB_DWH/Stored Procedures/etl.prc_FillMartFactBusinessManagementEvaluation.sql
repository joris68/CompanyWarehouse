CREATE PROCEDURE etl.prc_FillMartFactBusinessManagementEvaluation AS
BEGIN

	DROP TABLE IF EXISTS #tabTmp; 

	
	;WITH CTE_Prep AS
	(
		SELECT 
			LTRIM(RTRIM(NULLIF(Konto,''))) Account
			,CASE 
				WHEN Vorzeichen = 'S' THEN PARSE(Wert AS DECIMAL(18,2) USING 'de-DE')*-1
				WHEN Vorzeichen = 'H' THEN PARSE(Wert AS DECIMAL(18,2) USING 'de-DE')*1
				ELSE NULL
			END Balance
			,REPLACE(REPLACE(NameFile,'BWA Leistung ',''),' mit Konten','') AS Datumstring
		
		FROM [stg].[datevBWA]
		WHERE CONVERT(SMALLINT,NULLIF(FktSchl,'')) in 
		(
			0 
		)
	)
	, CTE AS
	(
		SELECT 
			CONVERT(NVARCHAR(500),CONCAT(a.Datumstring,'|',a.Account)) EvaluationTransactionIdentifier
			,ISNULL(b.[AccountId],-1) AS AccountId
			,ISNULL
			(
				cast(TRY_convert(char(8), 
					TRY_CONVERT(DATE
						,CONCAT(
							'01.'
							,LEFT(Datumstring,2)
							,'.20'
							,RIGHT(Datumstring,2)
						)
					)
				, 112) as int 
				)
			,1900101
			) AS DateId
			,Balance
		FROM CTE_Prep a
			LEFT JOIN mart.[DimAccount] b on a.Account = b.[Account]
	)

	SELECT *
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(EvaluationTransactionIdentifier,'N/A')),
                            '|'--,CONVERT(VARCHAR(30), ISNULL(KindKontoId,-1))
                            --'|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyBK
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), AccountId), --NULL Behandlung in Qry
                            '|',CONVERT(VARCHAR(30), DateId), --NULL Behandlung in Qry
                            '|',CONVERT(VARCHAR(30), ISNULL(Balance,'-1'))
                        )
                    ), 2)) AS HashKeyValue
	INTO #tabTmp
	FROM CTE



	MERGE mart.[FactBusinessManagementEvaluation] AS dst 
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
		EvaluationTransactionIdentifier
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
		EvaluationTransactionIdentifier
		,AccountId
		,[DateId]
		,Balance
		,GETDATE()
		,GETDATE()
		,[HashkeyBK]    
		,[HashkeyValue] 
		,0
		,GETDATE()
	)

	;

END
