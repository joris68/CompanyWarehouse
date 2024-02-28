CREATE TABLE [dwh].[DimBlueAntProjectState] (
    [ProjectStateID]   INT            IDENTITY (1, 1) NOT NULL,
    [ProjectStateBK]   INT            NOT NULL,
    [ProjectStateName] NVARCHAR (100) NOT NULL,
    [HashkeyBK]        VARCHAR (255)  NOT NULL,
    [HashkeyValue]     VARCHAR (255)  NOT NULL,
    [InsertDate]       DATETIME       NOT NULL,
    [UpdateDate]       DATETIME       NOT NULL,
    CONSTRAINT [PK_dwh_DimBlueAntProjectState_ProjectStateID] PRIMARY KEY CLUSTERED ([ProjectStateID] ASC)
);

