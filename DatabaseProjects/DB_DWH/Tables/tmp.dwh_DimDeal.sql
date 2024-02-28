CREATE TABLE [tmp].[dwh_DimDeal] (
    [DealBK]            VARCHAR (100)  NOT NULL,
    [DealName]          NVARCHAR (200) NOT NULL,
    [CustomerName]      NVARCHAR (200) NOT NULL,
    [DealStateID]       INT            NOT NULL,
    [DealPlannedDays]   DECIMAL (18)   NOT NULL,
    [LastChangedDateID] INT            NOT NULL,
    [InsertDate]        DATETIME       NOT NULL,
    [UpdateDate]        DATETIME       NOT NULL,
    [HashkeyBK]         VARCHAR (255)  NOT NULL,
    [HashkeyValue]      VARCHAR (255)  NOT NULL
);

