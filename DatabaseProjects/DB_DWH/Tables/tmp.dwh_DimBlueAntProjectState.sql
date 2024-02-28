CREATE TABLE [tmp].[dwh_DimBlueAntProjectState] (
    [ProjectStateBK]   INT            NOT NULL,
    [ProjectStateName] NVARCHAR (100) NOT NULL,
    [HashkeyBK]        VARCHAR (255)  NOT NULL,
    [HashkeyValue]     VARCHAR (255)  NOT NULL,
    [InsertDate]       DATETIME       NOT NULL,
    [UpdateDate]       DATETIME       NOT NULL
);

