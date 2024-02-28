CREATE TABLE [tmp].[dwh_FactBlueAntInvoice] (
    [InvoiceBK]                           INT            NOT NULL,
    [InvoiceNumber]                       NVARCHAR (50)  NOT NULL,
    [InvoiceDateID]                       INT            NOT NULL,
    [BlueAntInvoiceStateID]               INT            NOT NULL,
    [InvoiceAmount]                       DECIMAL (9, 2) NOT NULL,
    [StartDateID]                         INT            NOT NULL,
    [EndDateID]                           INT            NOT NULL,
    [BlueAntProjectID]                    NVARCHAR (50)  NOT NULL,
    [WorktimeAccountableInHours]          DECIMAL (9, 2) NOT NULL,
    [WorktimeTravelAccountableInHours]    DECIMAL (9, 2) NOT NULL,
    [WorktimeNotAccountableInHours]       DECIMAL (9, 2) NOT NULL,
    [WorktimeTravelNotAccountableInHours] DECIMAL (9, 2) NOT NULL,
    [InsertDate]                          DATETIME       NOT NULL,
    [UpdateDate]                          DATETIME       NOT NULL,
    [HashkeyBK]                           VARCHAR (255)  NOT NULL,
    [HashkeyValue]                        VARCHAR (255)  NOT NULL
);

