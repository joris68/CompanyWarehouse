CREATE TABLE [mart].[FactBusinessManagementEvaluation]
(
	EvaluationTransactionIdentifier  NVARCHAR(500) NOT NULL
    ,DateId                         INT    NOT NULL
    ,AccountId                        SMALLINT NOT NULL
    ,Balance                           DECIMAL(18,2) NULL
	,IsDeleted                    TINYINT NOT NULL
    ,DeleteDate                     DATETIME2(0)
    ,[InsertDate]                   DATETIME2(0) NOT NULL 
    ,[UpdateDate]                   DATETIME2(0) NOT NULL
    ,[HashkeyBK]                    VARCHAR (255) NOT NULL
    ,[HashkeyValue]                 VARCHAR (255) NOT NULL
)

GO
CREATE CLUSTERED INDEX CIX_mart_FactBusinessManagementEvaluation_DateId 
    ON [mart].[FactBusinessManagementEvaluation] (DateId)