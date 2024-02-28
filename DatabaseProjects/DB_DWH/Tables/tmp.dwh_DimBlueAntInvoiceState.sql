CREATE TABLE [tmp].[dwh_DimBlueAntInvoiceState] (
    [InvoiceStateBK]   INT           NOT NULL,
    [InvoiceStateName] VARCHAR (50)  NOT NULL,
    [HashkeyBK]        VARCHAR (255) NOT NULL,
    [HashkeyValue]     VARCHAR (255) NOT NULL,
    [InsertDate]       DATETIME      NOT NULL,
    [UpdateDate]       DATETIME      NOT NULL
);

