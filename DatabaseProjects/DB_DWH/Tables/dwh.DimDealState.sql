CREATE TABLE [dwh].[DimDealState] (
    [DealStateID]       INT            IDENTITY (1, 1) NOT NULL,
    [DealStateBK]       INT            NOT NULL,
    [DealStateName]     NVARCHAR (200) NOT NULL,
    [DealStatePipeline] NVARCHAR (200) NOT NULL,
    [InsertDate]        DATETIME       NOT NULL,
    [UpdateDate]        DATETIME       NOT NULL,
    [HashkeyBK]         VARCHAR (255)  NOT NULL,
    [HashkeyValue]      VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimDealState_DealStateID] PRIMARY KEY CLUSTERED ([DealStateID] ASC)
);

