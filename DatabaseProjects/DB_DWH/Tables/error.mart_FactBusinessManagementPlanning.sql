CREATE TABLE [error].[mart_FactBusinessManagementEvaluationPlanMissingAccountMapping]
(
	ErrorTransactionIdentifier  NVARCHAR(500) NOT NULL
	,IsDeleted                  TINYINT NOT NULL
    ,DeleteDate                 DATETIME2(0) NULL
	,InsertDate					DATETIME2(0) NOT NULL
   ,[UpdateDate] DATETIME2(0) NOT NULL, 
    CONSTRAINT [PK_mart_FactBusinessManagementEvaluationPlanMissingAccountMapping] PRIMARY KEY CLUSTERED ([ErrorTransactionIdentifier] ASC) 
)



   