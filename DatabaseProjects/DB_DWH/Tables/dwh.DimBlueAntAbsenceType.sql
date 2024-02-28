CREATE TABLE [dwh].[DimBlueAntAbsenceType] (
    [BlueAntAbsenceTypeID] INT            IDENTITY (1, 1) NOT NULL,
    [AbsenceTypeBK]        INT            NOT NULL,
    [AbsenceTypeName]      NVARCHAR (100) NULL,
    [HashkeyBK]            VARCHAR (255)  NOT NULL,
    [HashkeyValue]         VARCHAR (255)  NOT NULL,
    [InsertDate]           DATETIME       NOT NULL,
    [UpdateDate]           DATETIME       NOT NULL
);

