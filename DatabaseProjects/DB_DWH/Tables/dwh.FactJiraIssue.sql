CREATE TABLE [dwh].[FactJiraIssue] (
    [JiraIssueID]          INT           NULL,
    [JiraIssueBK]          INT           NULL,
    [OriginalTimeEstimate] INT           NULL,
    [TimeSpent]            INT           NULL,
    [CurrentTimeEstimate]  INT           NULL,
    [InsertDate]           DATETIME      NOT NULL,
    [UpdateDate]           DATETIME      NOT NULL,
    [HashkeyBK]            VARCHAR (255) NULL,
    [HashkeyValue]         VARCHAR (255) NULL
);

