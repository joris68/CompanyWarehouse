CREATE TABLE [tmp].[dwh_DimDealState] (
    [DealStateBK]       INT            NOT NULL,
    [DealStateName]     NVARCHAR (200) NOT NULL,
    [DealStatePipeline] NVARCHAR (200) NOT NULL,
    [InsertDate]        DATETIME       NOT NULL,
    [UpdateDate]        DATETIME       NOT NULL,
    [HashkeyBK]         VARCHAR (255)  NOT NULL,
    [HashkeyValue]      VARCHAR (255)  NOT NULL
);

