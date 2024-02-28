CREATE PROCEDURE [logging].[prc_LoadHashkeyBkDuplikatsStatus]
AS
BEGIN

	SET NOCOUNT ON;  

	TRUNCATE TABLE [tmp].[logging_HashkeyBkDuplikatsStatus];
  
	DECLARE @TABLE_NAME NVARCHAR(MAX) 
	DECLARE @TABLE_SCHEMA NVARCHAR(MAX) 
	DECLARE @Sql NVARCHAR(MAX)
	DECLARE @fqn NVARCHAR(MAX)
 

  
	DECLARE vendor_cursor CURSOR FOR   
		SELECT 
			CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) AS fqn
			,TABLE_NAME
			,TABLE_SCHEMA
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE COLUMN_NAME = N'Hashkeybk' -- get tables which are using the column Hashkeybk
			AND TABLE_SCHEMA != N'tmp' -- we do not cate aboput tmp schema tables
			and TABLE_NAME not like N'%bak%' --we do not care about bak tables
			AND TABLE_NAME not in (N'factplanningscd2') -- its SCD2 so we will have dupllicate Hashkeybks' (but only one RowIsCurrent = 1)
  
	OPEN vendor_cursor  
  
	FETCH NEXT FROM vendor_cursor   
		INTO @fqn,@TABLE_NAME,@TABLE_SCHEMA
  
	WHILE @@FETCH_STATUS = 0  
	BEGIN  
		SET @Sql= ''
	

		SET @Sql = N'

			WITH CTE AS
			(
				SELECT 
					'''+@TABLE_SCHEMA+'''	AS TableSchema
					,'''+@TABLE_NAME+'''	AS TableName
					,COUNT(1) HitDuplicateKey
					,ISNULL(SUM(Hits),0) SumDuplicates
					,0 AS IsDeleted
				FROM
				(
					SELECT Hashkeybk, count(1) Hits
					FROM '+@fqn+'
					GROUP BY Hashkeybk
					HAVING COUNT(1) > 1
				) a
			)
		
			INSERT INTO tmp.logging_HashkeyBkDuplikatsStatus
			(
				TableSchema
				,TableName
				,HitDuplicateKey
				,SumDuplicates
				,IsDeleted
				,HashKeyValue
				,HashkeyBK
			)
		

			SELECT 
				TableSchema
				,TableName
				,HitDuplicateKey
				,SumDuplicates
				,IsDeleted
				,LOWER(CONVERT(VARCHAR(64), HASHBYTES(''SHA2_256'', 
								CONCAT
								(
									CONVERT(VARCHAR(30), ISNULL(HitDuplicateKey,0))
									,''|'',CONVERT(VARCHAR(30), ISNULL(SumDuplicates,0))
									,''|'',CONVERT(VARCHAR(30), ISNULL(IsDeleted,0))
								)
							), 2)) AS HashKeyValue
				,LOWER(CONVERT(VARCHAR(64), HASHBYTES(''SHA2_256'', 
								CONCAT
								(
									CONVERT(VARCHAR(30), ISNULL(TableSchema,''N/A''))
									,''|'',CONVERT(VARCHAR(30), ISNULL(TableName,''N/A''))
								)
							), 2)) AS HashkeyBK
				FROM CTE

			'

	
		--print @sql
		exec sp_executesql @sql
  
		FETCH NEXT FROM vendor_cursor   
			INTO @fqn,@TABLE_NAME,@TABLE_SCHEMA
	END   

	CLOSE vendor_cursor;  
	DEALLOCATE vendor_cursor;  


	BEGIN TRANSACTION; 

	BEGIN TRY 

	BEGIN 

			MERGE [logging].[HashkeyBkDuplikatsStatus] dst 
			USING tmp.logging_HashkeyBkDuplikatsStatus src ON dst.HashkeyBK = src.HashkeyBK
			WHEN MATCHED AND dst.HashKeyValue != src.HashKeyValue THEN 
				UPDATE
				SET 
					TableSchema			= src.TableSchema
					,TableName			= src.TableName
					,HitDuplicateKey	= src.HitDuplicateKey
					,SumDuplicates		= src.SumDuplicates
					,HashKeyValue		= src.HashKeyValue
					,IsDeleted			= src.IsDeleted
					,UpdateDate			= GETDATE()
			WHEN NOT MATCHED BY TARGET THEN 
				INSERT
				(
					[TableSchema]
					, [TableName]
					, [HitDuplicateKey]
					, [SumDuplicates]
					, [HashKeyValue]
					, [HashkeyBK]
					, [IsDeleted]
					, [InsertDate]
					, [UpdateDate]
				)

				VALUES 
				(
					  src.[TableSchema]
					, src.[TableName]
					, src.[HitDuplicateKey]
					, src.[SumDuplicates]
					, src.[HashKeyValue]
					, src.[HashkeyBK]
					, src.IsDeleted
					, GETDATE()
					, GETDATE()

				)
			;

		
			/*
			* Soft deletes for those which are not existing anymore
			*
			*/ 
			UPDATE dst
			SET 
				dst.IsDeleted = 1
				,Dst.UpdateDate = GETDATE()
			FROM [logging].[HashkeyBkDuplikatsStatus] dst 
				LEFT JOIN tmp.logging_HashkeyBkDuplikatsStatus src ON dst.HashkeyBK = src.HashkeyBK
			WHERE src.HashkeyBK is null


 

		END 

 

	IF @@TRANCOUNT > 0 

	COMMIT TRANSACTION; 

	END TRY 

	BEGIN CATCH 

	IF @@TRANCOUNT > 0 

	ROLLBACK TRANSACTION; 

	THROW; 

	END CATCH; 
	



END
