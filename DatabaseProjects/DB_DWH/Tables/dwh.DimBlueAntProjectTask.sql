CREATE TABLE [dwh].[DimBlueAntProjectTask] (
    [BlueAntProjectTaskID] INT            IDENTITY (1, 1) NOT NULL,
    [BlueAntTaskBK]        INT            NULL,
    [BlueAntProjectID]     INT            NULL,
    [TaskName]             NVARCHAR (255) NULL,
    [TaskParent]           INT            NULL,
    [HashkeyBK]            VARCHAR (255)  NOT NULL,
    [HashkeyValue]         VARCHAR (255)  NOT NULL,
    [InsertDate]           DATETIME       NOT NULL,
    [UpdateDate]           DATETIME       NOT NULL,
    CONSTRAINT [PK_dwh_DimBlueAntProjectTask_BlueAntProjectTaskID] PRIMARY KEY CLUSTERED ([BlueAntProjectTaskID] ASC)
);

