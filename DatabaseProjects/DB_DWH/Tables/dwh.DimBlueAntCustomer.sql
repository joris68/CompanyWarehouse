CREATE TABLE [dwh].[DimBlueAntCustomer] (
    [BlueAntCustomerID] INT           IDENTITY (1, 1) NOT NULL,
    [CustomerBK]        INT           NOT NULL,
    [CustomerName]      VARCHAR (255) NOT NULL,
    [HashkeyBK]         VARCHAR (255) NOT NULL,
    [HashkeyValue]      VARCHAR (255) NOT NULL,
    [InsertDate]        DATETIME      NOT NULL,
    [UpdateDate]        DATETIME      NOT NULL,
    CONSTRAINT [PK_dwh_DimBlueAntCustomer_BlueAntCustomerID] PRIMARY KEY CLUSTERED ([BlueAntCustomerID] ASC)
);

