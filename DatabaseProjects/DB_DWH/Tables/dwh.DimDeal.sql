CREATE TABLE [dwh].[DimDeal] (
    [DealID]            INT            IDENTITY (1, 1) NOT NULL,
    [DealBK]            VARCHAR (100)  NOT NULL,
    [DealName]          NVARCHAR (200) NOT NULL,
    [CustomerName]      NVARCHAR (200) NOT NULL,
    [DealStateID]       INT            NOT NULL,
    [DealPlannedDays]   DECIMAL (18)   NOT NULL,
    [LastChangedDateID] INT            NOT NULL,
    [InsertDate]        DATETIME       NOT NULL,
    [UpdateDate]        DATETIME       NOT NULL,
    [HashkeyBK]         VARCHAR (255)  NOT NULL,
    [HashkeyValue]      VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimDeal_DealID] PRIMARY KEY CLUSTERED ([DealID] ASC)
);

