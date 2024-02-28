CREATE TABLE [tmp].[logging_HashkeyBkDuplikatsStatus]
(
	TableSchema					NVARCHAR(256)					NOT NULL
	,TableName					NVARCHAR(256)					NOT NULL
	,HitDuplicateKey			SMALLINT						NOT NULL
	,SumDuplicates				SMALLINT						NOT NULL
	,IsDeleted					SMALLINT						NOT NULL
	,HashKeyValue				NVARCHAR(255)					NOT NULL
	,HashkeyBK					NVARCHAR(255)					NOT NULL
	
)
