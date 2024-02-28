CREATE TABLE [dwh].[DimJiraIssueType] (
    [JiraIssueTypeID] INT           IDENTITY (1, 1) NOT NULL,
    [JiraIssueTypeBK] INT           NOT NULL,
    [JiraIssueType]   NVARCHAR (50) NOT NULL,
    [InsertDate]      DATETIME      NOT NULL,
    [UpdateDate]      DATETIME      NOT NULL,
    [HashkeyBK]       VARCHAR (255) NOT NULL,
    [HashkeyValue]    VARCHAR (255) NOT NULL
);

