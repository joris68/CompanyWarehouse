CREATE TABLE [mart].[DimAccount]
(
	[AccountId] SMALLINT IDENTITY NOT NULL
    ,[Account] NVARCHAR(50) NOT NULL
    ,[AccountName] NVARCHAR(500) NOT NULL
    ,[AccountReportingName] NVARCHAR(500) NOT NULL
    ,[IsLeaf]    TINYINT NOT NULL
    ,[InsertDate] DATETIME2(0) NOT NULL 
    ,[UpdateDate] DATETIME2(0) NOT NULL
    ,[HashkeyBK]            VARCHAR (255) NOT NULL
    ,[HashkeyValue]         VARCHAR (255) NOT NULL

    ,CONSTRAINT [PK_mart_DimAccount] PRIMARY KEY CLUSTERED ([AccountId] ASC)
)


GO

--20240222 wir müssen sicherstellen dass der AccountReportingName da wo er gefüllt ist nicht doppelt vorkommen darf
CREATE UNIQUE INDEX UNIX_mart_DimAccount_AccountReportingName
    ON mart.DimAccount     (AccountReportingName)
	WHERE AccountReportingName != N'N/A'