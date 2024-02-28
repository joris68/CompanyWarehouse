CREATE TABLE [dwh].[DimJiraSprint] (
    [JiraSprintID] INT           IDENTITY (1, 1) NOT NULL,
    [SprintBK]     INT           NOT NULL,
    [SprintName]   NVARCHAR (30) NOT NULL,
    [StartDateID]  INT           NOT NULL,
    [EndDateID]    INT           NOT NULL,
    [InsertDate]   DATETIME      NOT NULL,
    [UpdateDate]   DATETIME      NOT NULL,
    [HashkeyBK]    VARCHAR (255) NOT NULL,
    [HashkeyValue] VARCHAR (255) NOT NULL,
    CONSTRAINT [PK_dwh_DimJiraSprint_JiraSprintID] PRIMARY KEY CLUSTERED ([JiraSprintID] ASC)
);

