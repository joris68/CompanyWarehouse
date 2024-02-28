CREATE PROCEDURE [etl].[prc_FillMartRelAccountToAccount] AS
BEGIN

	DROP TABLE IF EXISTS #tabTmp; 

	;WITH CTE_DataPrep AS
	(
		SELECT 
		
			LTRIM(RTRIM(NULLIF(Zeile1,''))) ParentAccount
			,LTRIM(RTRIM(NULLIF(Konto,''))) ChildAccount
		FROM [stg].[datevBWA]
		WHERE CONVERT(SMALLINT,NULLIF(FktSchl,'')) = 0 --20240221 Annahme DBR das sind meine Konten

		UNION 
	
		SELECT 
			LTRIM(RTRIM(NULLIF(Zeile1,''))) ParentAccount
			,LTRIM(RTRIM(NULLIF(Zeile2,''))) ChildAccount
		FROM [stg].[datevBWA]
		WHERE CONVERT(SMALLINT,NULLIF(FktSchl,'')) in 
		(
			1 --20240221 Annahme DBR das ist meine erste Ebene
			,2 --20240221 Annahme DBR das ist meine zweite Ebene
		)

	),
	CTE AS
	(
		SELECT 
			a.*
			,CONCAT(a.ParentAccount,'|',a.ChildAccount) AS RelationTransactionIdentifier
			,b.AccountId AS ParentAccountIdId
			,c.AccountId AS ChildAccountIdId
		FROM CTE_DataPrep a
			LEFT JOIN mart.DimAccount b on a.ParentAccount = b.Account
			LEFT JOIN mart.DimAccount c on a.ChildAccount = c.Account
	)

	SELECT 
		RelationTransactionIdentifier
		,ParentAccountIdId
		,ChildAccountIdId 
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(RelationTransactionIdentifier,-1)),
                            '|'--,CONVERT(VARCHAR(30), ISNULL(ChildAccountIdId,-1))
                            --'|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyBK
		,LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(ParentAccountIdId,-1)),
                            '|',CONVERT(VARCHAR(30), ISNULL(ChildAccountIdId,-1))
                            --'|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyValue

	INTO #tabTmp
	FROM CTE
	WHERE ParentAccountIdId is not null AND ChildAccountIdId  is not null
	GROUP BY ParentAccountIdId,ChildAccountIdId ,RelationTransactionIdentifier


	MERGE mart.RelAccountToAccount AS dst 
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
		AND  dst.IsDeleted = 0
	THEN UPDATE SET
		IsDeleted = 1
		,DeleteDate = GETDATE()
		,HashkeyValue = 'N/A'

	WHEN NOT MATCHED BY TARGET 
	THEN INSERT
	(
		RelationTransactionIdentifier
		,[ParentAccountId]
		,[ChildAccountId]
		,[InsertDate]	
		,[UpdateDate]	
		,[HashkeyBK]    
		,[HashkeyValue] 
		,IsDeleted 
		,DeleteDate

	)
	VALUES
	(
		RelationTransactionIdentifier
		,[ParentAccountIdId]
		,[ChildAccountIdId]
		,GETDATE()
		,GETDATE()
		,[HashkeyBK]    
		,[HashkeyValue] 
		,0
		,GETDATE()
	)

	;

END
