CREATE TABLE [tmp].[dwh_DimBlueAntProject] (
    [ProjectNumber]   NVARCHAR (20)  NOT NULL,
    [ProjectLeaderID] INT            NOT NULL,
    [CustomerBK]      INT            NOT NULL,
    [ProjectBK]       INT            NOT NULL,
    [ProjectName]     NVARCHAR (255) NOT NULL,
    [StartDate]       DATE           NOT NULL,
    [EndDate]         DATE           NOT NULL,
    [ProjectStateBK]  NVARCHAR (100) NOT NULL,
    [InsertDate]      DATETIME       NOT NULL,
    [UpdateDate]      DATETIME       NOT NULL,
    [HashkeyBK]       VARCHAR (255)  NOT NULL,
    [HashkeyValue]    VARCHAR (255)  NOT NULL
);

