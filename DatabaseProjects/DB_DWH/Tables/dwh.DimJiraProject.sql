CREATE TABLE [dwh].[DimJiraProject] (
    [JiraProjectID]         INT            IDENTITY (1, 1) NOT NULL,
    [JiraProjectBK]         INT            NOT NULL,
    [JiraProjectCategoryID] INT            NOT NULL,
    [JiraProjectShortName]  NVARCHAR (50)  NOT NULL,
    [JiraProjectName]       NVARCHAR (255) NOT NULL,
    [InsertDate]            DATETIME       NOT NULL,
    [UpdateDate]            DATETIME       NOT NULL,
    [HashkeyBK]             VARCHAR (255)  NOT NULL,
    [HashkeyValue]          VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimJiraProject_JiraProjectID] PRIMARY KEY CLUSTERED ([JiraProjectID] ASC)
);

