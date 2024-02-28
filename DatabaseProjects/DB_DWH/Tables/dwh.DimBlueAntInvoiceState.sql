CREATE TABLE [dwh].[DimBlueAntInvoiceState] (
    [BlueAntInvoiceStateID] INT           IDENTITY (1, 1) NOT NULL,
    [InvoiceStateBK]        INT           NOT NULL,
    [InvoiceStateName]      VARCHAR (50)  NOT NULL,
    [InsertDate]            DATETIME      NOT NULL,
    [UpdateDate]            DATETIME      NOT NULL,
    [HashkeyBK]             VARCHAR (255) NOT NULL,
    [HashkeyValue]          VARCHAR (255) NOT NULL,
    CONSTRAINT [PK_dwh_DimBlueAntInvoiceState_BlueAntInvoiceStateID] PRIMARY KEY CLUSTERED ([BlueAntInvoiceStateID] ASC)
);

