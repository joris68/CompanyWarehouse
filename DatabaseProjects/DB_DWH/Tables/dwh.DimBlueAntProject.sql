CREATE TABLE [dwh].[DimBlueAntProject] (
    [BlueAntProjectID] INT            IDENTITY (1, 1) NOT NULL,
    [ProjectNumber]    NVARCHAR (20)  NOT NULL,
    [ProjectManagerID] INT            NOT NULL,
    [CustomerID]       INT            NOT NULL,
    [ProjectBK]        INT            NOT NULL,
    [ProjectName]      NVARCHAR (150) NOT NULL,
    [ProjectStateID]   INT            NOT NULL,
    [StartDateID]      INT            NOT NULL,
    [EndDateID]        INT            NOT NULL,
    [InsertDate]       DATETIME       NOT NULL,
    [UpdateDate]       DATETIME       NOT NULL,
    [HashkeyBK]        VARCHAR (255)  NOT NULL,
    [HashkeyValue]     VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimBlueAntProjectBackup_BlueAntProjectID] PRIMARY KEY CLUSTERED ([BlueAntProjectID] ASC)
);

