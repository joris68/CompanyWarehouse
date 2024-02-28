CREATE TABLE [dwh].[FactJiraWorklog] (
    [JiraWorklogID]        INT             NULL,
    [JiraIssueID]          INT             NULL,
    [JiraIssueBK]          INT             NULL,
    [JiraTimeSpentMinutes] INT             NULL,
    [WorklogSummary]       NVARCHAR (1000) NULL,
    [WorklogEntryDateID]   INT             NULL,
    [UserID]               INT             NULL,
    [InsertDate]           DATETIME        NOT NULL,
    [UpdateDate]           DATETIME        NOT NULL,
    [HashkeyBK]            VARCHAR (255)   NOT NULL,
    [HashkeyValue]         VARCHAR (255)   NOT NULL
);

