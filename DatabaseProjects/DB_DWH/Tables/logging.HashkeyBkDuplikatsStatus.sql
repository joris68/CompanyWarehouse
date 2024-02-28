CREATE TABLE [logging].[HashkeyBkDuplikatsStatus]
(
	[RowID]						INT           IDENTITY (1, 1)	NOT NULL
	,TableSchema				NVARCHAR(256)					NOT NULL
	,TableName					NVARCHAR(256)					NOT NULL
	,HitDuplicateKey			SMALLINT						NOT NULL
	,SumDuplicates				SMALLINT						NOT NULL
	,HashKeyValue				NVARCHAR(255)					NOT NULL
	,HashkeyBK					NVARCHAR(255)					NOT NULL
	,IsDeleted					SMALLINT						NOT NULL
	,InsertDate					DATETIME2(0)					NOT NULL
	,UpdateDate					DATETIME2(0)					NOT NULL, 
    CONSTRAINT [PK_logging_HashkeyBkDuplikatsStatus_RowID] PRIMARY KEY ([RowID])
)
