CREATE TABLE [mart].[RelAccountToAccount]
(
	RelationTransactionIdentifier  NVARCHAR(500) NOT NULL
    ,[ParentAccountId]     SMALLINT NOT NULL
	,[ChildAccountId]      SMALLINT NOT NULL
    ,IsDeleted        TINYINT NOT NULL
    ,DeleteDate         DATETIME2(0)
    ,[InsertDate]       DATETIME2(0) NOT NULL 
    ,[UpdateDate]       DATETIME2(0) NOT NULL
    ,[HashkeyBK]        VARCHAR (255) NOT NULL
    ,[HashkeyValue]     VARCHAR (255) NOT NULL

    ,CONSTRAINT [PK_mart_RelAccountToAccount_RelationTransactionIdentifier] PRIMARY KEY CLUSTERED (
        RelationTransactionIdentifier
        )
)
